build:
  cargo xtask build

build_zig:
  cargo xtask build zig

init_service:
  cargo xtask service init

start_service:
  cargo xtask service start

check_service:
  cargo xtask service status

test_s3_api:
  cargo xtask precheckin --s3_api_only

precheckin:
  cargo xtask precheckin

deploy:
  cargo xtask deploy

describe_stack:
  cargo xtask tools describe_stack

git_status:
  cargo xtask git status

git_each_status:
  cargo xtask git foreach git status

git_pull_rebase:
  cargo xtask git foreach git pull -- --rebase

