echo Sourced jenkins-common.sh version 41
###
### Colors
###

export COLOR_OFF=$'\033[0m'       # Text Reset
# Regular Colors
export COLOR_BLACK=$'\033[0;30m'        # Black
export COLOR_RED=$'\033[0;31m'          # Red
export COLOR_GREEN=$'\033[0;32m'        # Green
export COLOR_YELLOW=$'\033[0;33m'       # Yellow
export COLOR_BLUE=$'\033[0;34m'         # Blue
export COLOR_PURPLE=$'\033[0;35m'       # Purple
export COLOR_CYAN=$'\033[0;36m'         # Cyan
export COLOR_WHITE=$'\033[0;37m'        # White

# Bold
export COLOR_BBLACK=$'\033[1;30m'       # Black
export COLOR_BRED=$'\033[1;31m'         # Red
export COLOR_BGREEN=$'\033[1;32m'       # Green
export COLOR_BYELLOW=$'\033[1;33m'      # Yellow
export COLOR_BBLUE=$'\033[1;34m'        # Blue
export COLOR_BPURPLE=$'\033[1;35m'      # Purple
export COLOR_BCYAN=$'\033[1;36m'        # Cyan
export COLOR_BWHITE=$'\033[1;37m'       # White

# Underline
export COLOR_UBLACK=$'\033[4;30m'       # Black
export COLOR_URED=$'\033[4;31m'         # Red
export COLOR_UGREEN=$'\033[4;32m'       # Green
export COLOR_UYELLOW=$'\033[4;33m'      # Yellow
export COLOR_UBLUE=$'\033[4;34m'        # Blue
export COLOR_UPURPLE=$'\033[4;35m'      # Purple
export COLOR_UCYAN=$'\033[4;36m'        # Cyan
export COLOR_UWHITE=$'\033[4;37m'       # White

# Background
export COLOR_ON_BLACK=$'\033[40m'       # Black
export COLOR_ON_RED=$'\033[41m'         # Red
export COLOR_ON_GREEN=$'\033[42m'       # Green
export COLOR_ON_YELLOW=$'\033[43m'      # Yellow
export COLOR_ON_BLUE=$'\033[44m'        # Blue
export COLOR_ON_PURPLE=$'\033[45m'      # Purple
export COLOR_ON_CYAN=$'\033[46m'        # Cyan
export COLOR_ON_WHITE=$'\033[47m'       # White

# High Intensity
export COLOR_IBLACK=$'\033[0;90m'       # Black
export COLOR_IRED=$'\033[0;91m'         # Red
export COLOR_IGREEN=$'\033[0;92m'       # Green
export COLOR_IYELLOW=$'\033[0;93m'      # Yellow
export COLOR_IBLUE=$'\033[0;94m'        # Blue
export COLOR_IPURPLE=$'\033[0;95m'      # Purple
export COLOR_ICYAN=$'\033[0;96m'        # Cyan
export COLOR_IWHITE=$'\033[0;97m'       # White

# Bold High Intensity
export COLOR_BIBLACK=$'\033[1;90m'      # Black
export COLOR_BIRED=$'\033[1;91m'        # Red
export COLOR_BIGREEN=$'\033[1;92m'      # Green
export COLOR_BIYELLOW=$'\033[1;93m'     # Yellow
export COLOR_BIBLUE=$'\033[1;94m'       # Blue
export COLOR_BIPURPLE=$'\033[1;95m'     # Purple
export COLOR_BICYAN=$'\033[1;96m'       # Cyan
export COLOR_BIWHITE=$'\033[1;97m'      # White

# High Intensity backgrounds
export COLOR_ON_IBLACK=$'\033[0;100m'   # Black
export COLOR_ON_IRED=$'\033[0;101m'     # Red
export COLOR_ON_IGREEN=$'\033[0;102m'   # Green
export COLOR_ON_IYELLOW=$'\033[0;103m'  # Yellow
export COLOR_ON_IBLUE=$'\033[0;104m'    # Blue
export COLOR_ON_IPURPLE=$'\033[0;105m'  # Purple
export COLOR_ON_ICYAN=$'\033[0;106m'    # Cyan
export COLOR_ON_IWHITE=$'\033[0;107m'   # White

# Always leave new line

###
### Logging
###

log() {
  printf "%s %s %s %s\n" "$(date +'%Y-%m-%d %H:%M:%S')" "${COLOR_BLACK}" "${*}" "${COLOR_OFF}"
}

log_info() {
  printf "%s %s %s %s\n" "$(date +'%Y-%m-%d %H:%M:%S')" "${COLOR_BLUE}INFO:" "${*}" "${COLOR_OFF}"
}

log_error() {
  printf "%s %s %s %s\n" "$(date +'%Y-%m-%d %H:%M:%S')" "${COLOR_RED}ERROR:" "${*}" "${COLOR_OFF}"
}

log_warn() {
  printf "%s %s %s %s\n" "$(date +'%Y-%m-%d %H:%M:%S')" "${COLOR_YELLOW}WARN:" "${*}" "${COLOR_OFF}"
}

log_success() {
  printf "%s %s %s %s\n" "$(date +'%Y-%m-%d %H:%M:%S')" "${COLOR_GREEN}SUCCESS:" "${*}" "${COLOR_OFF}"
}

# Always leave new line

###
### Docker Container Images
###

# Export infra registries
export ARTIFACTORY_CACHE_REG_URL="docker.artifactory.ihme.washington.edu"
export INFRA_PUB_REG_URL="docker-infrapub.artifactory.ihme.washington.edu"
export INFRA_REG_URL="docker-infra.artifactory.ihme.washington.edu"
export SCICOMP_REG_URL="docker-scicomp.artifactory.ihme.washington.edu"


# Export variables for commonly used containers
export BASH_CONTAINER="${ARTIFACTORY_CACHE_REG_URL}/bash:latest"
export GRAPHVIZ_CONTAINER="${INFRA_PUB_REG_URL}/graphviz:latest"
export KUBECTL_CONTAINER="${INFRA_PUB_REG_URL}/kubectl:latest"
export MKDOCS_CONTAINER="${INFRA_PUB_REG_URL}/mkdocs-material:latest"
export YASHA_CONTAINER="${INFRA_PUB_REG_URL}/yasha:latest"

# build_push_container
# Usage: build_push_container <Reg Hostname> <Reg user> <Reg Password> <Container Tag>
build_push_container()
{
  REG_URL="$1"
  REG_USER="$2"
  REG_PASS="$3"
  IMAGE_NAME="$4"
  if [[ -z "$5" ]]; then
    DOCKERFILE_NAME="Dockerfile"
  else
    DOCKERFILE_NAME="$5"
  fi

  log_info Logging into: "${REG_URL}"
  docker login -u "${REG_USER}" -p "${REG_PASS}" https://"${REG_URL}"
  log_info Building: "${IMAGE_NAME}"
  docker build --no-cache -t "${IMAGE_NAME}" -f ${DOCKERFILE_NAME} .
  log_info Pushing: "${IMAGE_NAME}"
  docker push "${IMAGE_NAME}"
}

build_push_container_with_cache()
{
  REG_URL="$1"
  REG_USER="$2"
  REG_PASS="$3"
  IMAGE_NAME="$4"
  if [[ -z "$5" ]]; then
    DOCKERFILE_NAME="Dockerfile"
  else
    DOCKERFILE_NAME="$5"
  fi

  log_info Logging into: "${REG_URL}"
  docker login -u "${REG_USER}" -p "${REG_PASS}" https://"${REG_URL}"
  log_info Building: "${IMAGE_NAME}"
  docker build -t "${IMAGE_NAME}" -f ${DOCKERFILE_NAME} .
  log_info Pushing: "${IMAGE_NAME}"
  docker push "${IMAGE_NAME}"
}

docker_image_size()
{
  # Output size of a given image
  CONTAINER_IMAGE="$1"
  if [[ -z "${CONTAINER_IMAGE}" ]]; then
    log_error "docker_image_size: No Image Specified"
  else
    docker images ${CONTAINER_IMAGE} --format "{{.Size}}"
  fi
}

pull_common_container_images()
{
  log_info "Pulling Common Docker Container Images"
  echo -e "${COLOR_CYAN}"
  docker pull "${BASH_CONTAINER}"
  docker pull "${GRAPHVIZ_CONTAINER}"
  docker pull "${KUBECTL_CONTAINER}"
  docker pull "${MKDOCS_CONTAINER}"
  docker pull "${YASHA_CONTAINER}"
  echo -e "${COLOR_OFF}"
}

# Always leave new line
###
### Cleanup Functions
###

# fix_ownership, run chown in a container
# to set uid and gid to jenkins user
fix_ownership()
{
  # Fix ownership, clean old files
  log_info "Fixing Workspace File Ownership"
  docker run \
  -v "${WORKSPACE}":/workspace \
  "${BASH_CONTAINER}" \
  chown -R "$(id -u):$(id -g)" /workspace
}


# Use git to clean workspace, by deleting
# untracked files
delete_files_not_in_git()
{
  fix_ownership
  log_info "Deleting files not in git"
  git clean -xdf
}

# Always leave new line

###
### Post to Slack
###

# https://github.com/sulhome/bash-slack/blob/master/postToSlack.sh
# MIT License (MIT)

postToSlackUsage() {
    programName=$0
    echo "description: use this program to post messages to Slack channel"
    echo "Dependancies curl, bash"
    echo "usage: $programName [-t \"sample title\"] [-b \"message body\"] [-c \"mychannel\"] [-s \"messageColor\"] [-u \"slack url\"]"
    echo "  -t    the title of the message you are posting"
    echo "  -b    The message body"
    echo "  -c    The channel you are posting to"
    echo "  -s    The color of the message: good, warning, danger or a hex code eg. #439FE0"
    echo "  -e    Emoji to post as"
    echo "  -f    From name, defaults to local hostname"
    echo "  -u    The slack hook url to post to"
    return 1
}

postToSlack() {
    # Defaults
    icon_emoji="sunglasses"
    messageColor="#000000"
    slackFromName=$(hostname -s)

    while getopts ":t:b:c:s:e:u:h:f" opt; do
      case ${opt} in
        t) msgTitle="$OPTARG"
        ;;
        u) slackUrl="$OPTARG"
        ;;
        b) msgBody="$OPTARG"
        ;;
        c) channelName="$OPTARG"
        ;;
        s) messageColor="$OPTARG"
        ;;
        e) icon_emoji="$OPTARG"
        ;;
        f) slackFromName="$OPTARG"
        ;;
        h) postToSlackUsage
        ;;
        \?) echo "Invalid option -$OPTARG" >&2
        ;;
      esac
    done

payLoad=$(cat <<EOF
{
        "channel": "#${channelName}",
        "username": "${slackFromName}",
        "icon_emoji": ":${icon_emoji}:",
        "attachments": [
            {
                "fallback": "${msgTitle}",
                "color": "${messageColor}",
                "title": "${msgTitle}",
                "fields": [{
                    "title": "message",
                    "value": "${msgBody}",
                    "short": false
                }]
            }
        ]
    }
EOF
)
    statusCode=$(curl \
            --write-out %{http_code} \
            --silent \
            --output /dev/null \
            -X POST \
            -H 'Content-type: application/json' \
            --data "${payLoad}" ${slackUrl})

    log_info "Slack Post Status Code" ${statusCode}
    # check if the http status code is between 200 and 300
    if [ "$statusCode" -gt "199" ]
    then
        if [ "$statusCode" -lt "300" ]
        then
            return 0
        fi
    fi
    # otherwise return the statuscode as the function exit code
    return ${statusCode}
}

# Always leave new line
###
### Last File
###

export JENKINS_COMMON_SOURCED=true
