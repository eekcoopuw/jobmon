upload_python_dist () {
    WORKSPACE=$1
    REG_USERNAME=$2
    REG_PASSWORD=$3
    ACTIVATE=$4

    $ACTIVATE && nox --session build
    PYPI_URL="https://artifactory.ihme.washington.edu/artifactory/api/pypi/pypi-shared"
    JOBMON_VERSION=$(basename $(find ./dist/jobmon-*.tar.gz) | sed "s/jobmon-\\(.*\\)\\.tar\\.gz/\\1/")
    if [[ "$JOBMON_VERSION" =~ "dev" ]]
    then
      $ACTIVATE && twine upload \
        --repository-url $PYPI_URL \
        --username $REG_USERNAME \
        --password $REG_PASSWORD \
        --skip-existing \
        ./dist/*
    else
      $ACTIVATE && twine upload \
        --repository-url $PYPI_URL \
        --username $REG_USERNAME \
        --password $REG_PASSWORD \
        ./dist/*
    fi
    echo "$JOBMON_VERSION" > $WORKSPACE/jobmon_version_deployed.txt
    echo "Jobmon v$JOBMON_VERSION deployed to Pypi"

}
