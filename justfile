build *args:
  cargo xtask build {{args}}

service *args:
  cargo xtask service {{args}}

precheckin *args:
  cargo xtask precheckin {{args}}

deploy *args:
  cargo xtask deploy {{args}}

describe-stack *args:
  cargo xtask tools describe-stack {{args}}

dump-vg-config *args:
  cargo xtask tools dump-vg-config {{args}}

repo *args:
  cargo xtask git {{args}}

git *args:
  cargo xtask git foreach git {{args}}
