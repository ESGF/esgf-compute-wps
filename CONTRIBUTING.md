# How to contribute

Welcome to the project, we look forward to you contributing.

Here are some important resources:
* Bugs? Create issues [here](https://github.com/ESGF/esgf-compute-wps/issues/new)

## Testing

As you contribute features please write unittests.

We use pytest as our test runner.

```
conda env update -f cwt/tests/environment.yml

pytest
```

## Submitting changes

Changes should be made in a branch based on the devel branch. Once your done adding your feature and unittests 
create a [pull request](https://github.com/ESGF/esgf-compute-wps/compare), after review the feature will be 
merged back into the devel branch.

## Coding conventions

We use [flake8](https://pypi.org/project/flake8/) to keep consistent styling.

## Branching workflow 

We follow the [GitFlow](https://nvie.com/posts/a-successful-git-branching-model/) model.
See below for a quick summary.

* Main branches
  * master: Contains latest stable production code
  * devel: Contains latest development code
* Supporting branches
  * [feature](#feature)
  * [release](#release)
  * [hotfix (bugfix)](hotfix)

### Feature

* Based on devel branch
* Merge changes into devel branch

```
git checkout -b <feature> devel
... Make changes
git checkout devel
git merge <feature>
```

### Release

* Base on devel branch
* Merge changes into devel and master branch
* Create tag (vX.X.X) based on current master

```
git checkout -b <release> devel
... Make changes
git checkout devel
git merge <release>
git checkout master
git merge <release>

git tag <tag>
```

### Hotfix

* Base on tag that requires fix
* Merge changes into devel and master branch
* Create tag (vX.X.X) based on current master

```
git checkout -b <hotfix> <tag>
... Make changes
git checkout devel
git merge <release>
git checkout master
git merge <release>

git tag <new tag>
```
