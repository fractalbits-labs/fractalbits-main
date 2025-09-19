build *args:
  cargo xtask build {{args}}

service *args:
  cargo xtask service {{args}}

precheckin *args:
  cargo xtask precheckin {{args}}

deploy *args:
  cargo xtask deploy {{args}}

describe-stack:
  cargo xtask tools describe-stack

repo *args:
  cargo xtask git {{args}}

git *args:
    @cargo xtask git foreach git {{args}}

