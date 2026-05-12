# Release

Releases of this library are currently handled by [sbt-ci-release](https://github.com/sbt/sbt-ci-release).
Please see its documentation for more details about how it works if you are interested to know more.

The actual deployments are triggered manually by the maintainers of this repository, using `workflow_dispatch` event
trigger.

Once changes from a PR were reviewed and merged into the master branch, follow these steps:
1. Create a new Git Tag and push it to the repository, to the master branch. For example,
   if you want to release a version 0.4.0 (note that we are using [Semantic Versioning](https://semver.org/)):

    ```shell
    git tag -a v0.4.0 -m "v0.4.0"
    git push origin v0.4.0
    ```

2. In GitHub UI, go to the repository's **Actions** -> **Release** -> **Run workflow**, and under **Use workflow from**
   use **Tags** and find the tag you created in the previous step.

   > **Important note**: don't run the workflow against the master branch, but against the tag.
   > `sbt-ci-release` plugin won't be able to correctly find tag, and it will think that you are trying
   > to do a snapshot release, not an actual release that should be synchronized with Maven Central.
