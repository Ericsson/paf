#!/usr/bin/python3

#
# check-release.py -- script to verify a paf release
#
# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2023 Ericsson AB
#

import getopt
import git
import os
import re
import subprocess
import sys

from functools import total_ordering


def usage(name):
    print("%s [-c <cmd>] <release-sha|release-tag>" % name)
    print("Options:")
    print("  -c <cmd>  Run command <cmd>. Default is to run all.")
    print("  -h        Print this text.")
    print("Commands:")
    print("  meta     Only check release meta data.")
    print("  changes  Only list changes with previous release.")
    print("  test     Only run the test suites.")


def prefix(msg, *args):
    print("%s: " % msg, end="")
    print(*args, end="")
    print(".")


def fail(*args):
    prefix("ERROR", *args)
    sys.exit(1)


def note(*args):
    prefix("NOTE", *args)


@total_ordering
class Version:
    def __init__(self, major, minor, patch=None):
        self.major = major
        self.minor = minor
        self.patch = patch

    def __str__(self):
        s = "%d.%d" % (self.major, self.minor)
        if self.patch is not None:
            s += ".%d" % self.patch
        return s

    def __lt__(self, other):
        if self.major == other.major:
            if self.minor == other.minor:
                if self.patch is None:
                    assert other.patch is None
                    return False
                return self.patch < other.patch
            return self.minor < other.minor
        else:
            return self.major < other.major

    def __eq__(self, other):
        if self.major != other.major or  \
           self.minor != other.minor:
            return False
        if self.patch is None:
            return other.patch is None
        else:
            if other.patch is None:
                return False
            return self.patch == other.patch


setup_version_re = \
    re.compile(r'version="([0-9]+).([0-9]+)\.([0-9]+)"')


def get_setup_version(commit):
    fileobj = commit.tree / 'setup.py'
    data = fileobj.data_stream.read().decode('utf-8')
    m = setup_version_re.search(data)

    if m is None:
        fail("Version information not found in setup.py")

    major = int(m.groups()[0])
    minor = int(m.groups()[1])
    patch = int(m.groups()[2])

    return Version(major, minor, patch)


server_major_version_re = \
    re.compile(r'MAJOR_VERSION *= *([0-9]+)')
server_minor_version_re = \
    re.compile(r'MINOR_VERSION *= *([0-9]+)')
server_patch_version_re = \
    re.compile(r'PATCH_VERSION *= *([0-9]+)')


def get_server_version(commit):
    fileobj = commit.tree / 'paf/server.py'
    data = fileobj.data_stream.read().decode('utf-8')

    major = server_major_version_re.search(data)
    if major is None:
        fail("Server major version not found")

    minor = server_minor_version_re.search(data)
    if minor is None:
        fail("Server minor version not found")

    patch = server_patch_version_re.search(data)
    if patch is None:
        fail("Server patch version not found")

    return Version(int(major.groups()[0]),
                   int(minor.groups()[0]),
                   int(patch.groups()[0]))


def get_release_tags(repo):
    return [t for t in repo.tags if tag_re.match(t.name)]


def get_commit_release_tags(repo, commit):
    return [tag for tag in get_release_tags(repo) if tag.commit == commit]


def get_commit_release_tag(repo, commit):
    tags = get_commit_release_tags(repo, commit)

    if len(tags) != 1:
        fail("Could not find exactly one release tag for commit %s" %
             release_commit)

    return tags[0]


def get_release_versions(repo):
    return [get_tag_version(tag) for tag in get_release_tags(repo)]


def get_prev_release_tag(repo, release_version):

    release_tags = get_release_tags(repo)

    candidate = None

    for tag in release_tags[1:]:
        v = get_tag_version(tag)

        if v.major != release_version.major or \
           v.minor > release_version.minor:
            continue

        if v.minor == release_version.minor and \
           v.patch >= release_version.patch:
            continue

        if candidate is None:
            candidate = tag
            continue

        candidate_v = get_tag_version(candidate)
        if v.minor > candidate_v.minor:
            candidate = tag
            continue

        if v.minor == candidate_v.minor and v.patch > candidate_v.patch:
            candidate = tag
            continue

    if candidate is None:
        fail("Unable to find the release previous to %s" % release_version)

    return candidate


tag_re = re.compile('^v[0-9]+')


def tag_name(version):
    return "v%s" % version


def get_tag_version(tag):
    assert tag.name[0] == 'v'
    v = tag.name[1:].split('.')
    major = int(v[0])
    minor = int(v[1])
    patch = int(v[2])
    return Version(major, minor, patch)


def check_against_previous(this_version, prev_version):
    if this_version < prev_version:
        fail("Previous version \"%s\" is higher than this version \"%s\"" % \
             (prev_version, this_version))


def check_meta(repo, release_commit):
    release_tag = get_commit_release_tag(repo, release_commit)

    setup_version = get_setup_version(release_commit)
    server_version = get_server_version(release_commit)
    tag_version = get_tag_version(release_tag)

    if tag_version != setup_version:
        fail("Version according to tag and according to setup.py differ")

    if tag_version != server_version:
        fail("Version according to tag and according to server.py differ")

    prev_release_tag = get_prev_release_tag(repo, tag_version)
    prev_setup_version = get_setup_version(prev_release_tag.commit)

    print("Release information:")
    print("  Version (from setup.py): %s" % setup_version)
    print("  Commit:")
    print("    SHA: %s" % release_commit.hexsha)
    print("    Summary: %s" % release_commit.summary)
    print("  Previous release: %s" % prev_setup_version)
    print("Releases:")
    for version in get_release_versions(repo):
        print("  %s" % version)

    check_against_previous(setup_version, prev_setup_version)


def check_changes(repo, release_commit):
    release_tag = get_commit_release_tag(repo, release_commit)

    prev_release_tag = get_prev_release_tag(repo, get_tag_version(release_tag))

    rev = '%s..%s' % (prev_release_tag, release_tag)

    print("Changes between %s and %s:" % (get_tag_version(prev_release_tag),
                                          get_tag_version(release_tag)))
    for commit in repo.iter_commits(rev=rev):
        print("  %s" % commit.summary)


def run(cmd):
    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, encoding='utf-8')

    if res.returncode != 0:
        sys.stderr.write(res.stdout)
        sys.exit(1)


EXTRA_CFLAGS="-Werror"

def test_build_separate_build_dir(repo, release_commit):
    release_tag = get_commit_release_tag(repo, release_commit)

    print("Test build w/ separate build directory.")
    cmd = """
set -e
tmpdir=`mktemp -d`; \\
libpafdir=libpaf-%s; \\
tarfile=$tmpdir/$libpafdir.tar; \\
git archive --prefix=$libpafdir/ --format=tar -o $tarfile %s; \\
cd $tmpdir; \\
tar xf $tarfile; \\
cd $libpafdir; \\
autoreconf -i; \\
mkdir build; \\
cd build; \\
../configure; \\
make -j; \\
""" % (get_tag_version(release_tag), release_commit)

    run(cmd)

def run_test(repo, release_commit):
    release_tag = get_commit_release_tag(repo, release_commit)

    print("Running test suite for %s." % release_tag)

    cmd = """
set -e
tmpdir=`mktemp -d`; \\
pafdir=paf-%s; \\
tarfile=$tmpdir/$pafdir.tar; \\
git archive --prefix=$pafdir/ --format=tar -o $tarfile %s;\\
cd $tmpdir; \\
tar xf $tarfile; \\
cd $pafdir; \\
make check; \\
""" % (get_tag_version(release_tag), release_commit)

    run(cmd)

    print("OK")

def check_repo(repo):
    if repo.is_dirty():
        fail("Repository contains modifications")


optlist, args = getopt.getopt(sys.argv[1:], 'c:mh')

cmd = None

for opt, optval in optlist:
    if opt == '-h':
        usage(sys.argv[0])
        sys.exit(0)
    if opt == '-c':
        cmd = optval

if len(args) != 1:
    usage(sys.argv[0])
    sys.exit(1)

repo = git.Repo()
check_repo(repo)

release_commit = repo.commit(args[0])

meta = False
changes = False
test = False

if cmd == 'meta':
    meta = True
elif cmd == 'changes':
    changes = True
elif cmd == 'test':
    test = True
elif cmd is None:
    meta = True
    changes = True
    test = True
else:
    print("Unknown cmd '%s'." % cmd)
    sys.exit(1)

if meta:
    check_meta(repo, release_commit)
if changes:
    check_changes(repo, release_commit)
if test:
    run_test(repo, release_commit)
