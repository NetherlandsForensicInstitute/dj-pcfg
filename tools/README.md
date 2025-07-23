About
-
Collection of hacked together tools (1 at the moment) for [PCFG](https://github.com/lakiw/pcfg_cracker).

Requirements
-
* python >= 3.11

Setup
-
Install python (3.11 or later):
```
$ apt install python3.11 python3.11-venv
```
Setup project:
```
$ python3.11 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

modify_pcfg_rule
---
---
Used for editing a trained PCFG rule. Useful for improving the generating speed, reducing the output size or removing structures which would generate
invalid passwords for a given target, among other things.

**Experimental**, use at own risk!

Example usage:
```shell
$ python modify_pcfg_rule.py -b 32 -m 0.01 -p 32 -g 12 --remove-base-structure-if "'A' not in base_structure" Default DefaultModified
```

TODO
-
- [ ] explain more