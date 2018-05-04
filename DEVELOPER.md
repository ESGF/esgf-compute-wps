# Developers Guide

### Styling

Follow the Google python [style guide](https://google.github.io/styleguide/pyguide.html)

### Git Workflow

> *master* branch will hold production ready code

> *devel* branch will hold latest code

> Should use --no-ff flag when merging branches.

#### Feature branch

* Branch from devel
* Merge with devel
* Naming can be anything except master, devel, bugfix-*, release-*

e.g.

```
git checkout -b xml_rewrite devel
git checkout devel
git merge --no-ff xml_rewrite
git branch -d xml_rewrite
git push origin devel
```

#### Release branch

* Branch from devel
* Merge with devel and master
* Naming release-*

e.g.

```
git checkout -b release-2.1.0 devel
git checkout master
git merge --no-ff release-2.1.0
git tag -a 2.0.1
git checkout devel
git merge --no-ff release-2.1.0
git branch -d release-2.1.0
```

#### Bugfix branch

> If a release branch exists merge into that branch which will eventually make
> it into master and devel.

* Branch from master
* Merge with devel and master
* Naming bugfix-*

e.g.

```
git checkout -b bugfix-2.1.1 master
git checkout master
git merge --no-ff bugfix-2.1.1
git tag -a 2.1.1
git checkout devel
git merge --no-ff bugfix-2.1.1
git branch -d bugfix-2.1.1
```
