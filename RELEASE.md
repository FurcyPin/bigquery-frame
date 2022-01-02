Make sure the version matches the upstream version and increase the last digit in the version number

### Bump version

```
# 0.1.0 -> 0.1.1
bumpversion patch

# 0.1.1 -> 0.2.0
bumpversion minor
```


### Build everything

```
poetry build
```


### Publish

```
poetry publish
```