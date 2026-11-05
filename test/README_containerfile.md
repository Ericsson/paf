# run tests in a container

podman build -t paf-test -f test/containerfile .
podman run --rm paf-test