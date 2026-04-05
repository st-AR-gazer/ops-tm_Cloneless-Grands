"""Vendored subset of gbx-py used by this repo.

Upstream project:
https://github.com/schadocalex/gbx-py

Reference checked during local fixes:
dev @ 342419ce4e937a4303b8a36fc2d3ed8093d95cf5

This vendor is not a clean mirror of upstream. Local changes include:
- preserving chunk 0x03043040 as raw bytes,
- adding missing TM2020 collection ids,
- preserving collection ids when gbx-py decodes them as U<number>.
"""
