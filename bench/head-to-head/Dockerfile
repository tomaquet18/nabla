# Head-to-head benchmark image: the nabla development image plus pg_ivm,
# both built against the same PostgreSQL 17 server and headers.
#
#   docker build -t nabla-h2h:17 bench/head-to-head
#
# nabla itself is NOT baked in: run.sh installs it from the working tree with
# cargo pgrx install, the same way every other script in this repository does.
FROM nabla-dev:17

USER root

ARG PG_IVM_REF=v1.15
ARG PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config

# pg_ivm is a plain PGXS extension; the dev image already carries
# build-essential and postgresql-server-dev-17. The release tarball is used
# rather than a clone so the build needs nothing but HTTPS.
RUN mkdir -p /tmp/pg_ivm \
 && curl -fsSL "https://github.com/sraoss/pg_ivm/archive/refs/tags/${PG_IVM_REF}.tar.gz" \
      | tar xz --strip-components=1 -C /tmp/pg_ivm \
 && printf 'pg_ivm %s from https://github.com/sraoss/pg_ivm (tarball)\n' "${PG_IVM_REF}" > /opt/pg_ivm-version \
 && cd /tmp/pg_ivm \
 && make PG_CONFIG="${PG_CONFIG}" \
 && make install PG_CONFIG="${PG_CONFIG}" \
 && cd / && rm -rf /tmp/pg_ivm

USER dev
WORKDIR /work
CMD ["bash"]
