# Documentation

Documentation for this project is hosted [here](http://dev-tomflem.ihme.washington.edu/docs/jobmon/current/index.html)

To get started using Jobmon, just follow the instructions under the Quickstart heading

# For Jobmon Developers: Deploying to jobmon-p01
To deploy a centralized JobStateManager and JobQueryServer:

1. Login to jobmon-p01
2. Clone this repo into a folder called "jobmon_cavy"
```
git clone ssh://git@stash.ihme.washington.edu:7999/cc/jobmon.git jobmon_cavy
```

3. Checkout the appropriate branch (as of this writing, future/service_arch)
```
git checkout future/service_arch
```

4. From the root directory of the repo, run:
```
docker-compose up --build -d
```

That should do it.


# Deployment architecture
![deploy_arch_diagram](https://hub.ihme.washington.edu/download/attachments/44702059/Screen%20Shot%202017-10-18%20at%202.49.30%20PM.png?version=1&modificationDate=1508363448371&api=v2)


## Dependencies
- pyzmq
- pandas
- sqlalchemy
- numpy
- pymysql
- pyyaml
- drmaa
- jsonpickle
- subprocess32
