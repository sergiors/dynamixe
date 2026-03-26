.PHONY: build publish test-publish clean

build:
	uv build

clean:
	rm -rf dist/ build/ *.egg-info

test-publish: build
	uv publish --publish-url https://test.pypi.org/legacy/
