About
-
Collection of hacked together tools for [PCFG](https://github.com/lakiw/pcfg_cracker).

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
$ python modify_pcfg_rule.py -m 0.1 -p 32 -g 12 --include-casing-in-grammar-length --remove-base-structure-if 'all(c in "D01234567890" for c in base_structure)' Default DefaultModified
```

This does a couple of things:
- Remove all password patterns consisting of only digits (which will probably have been tried already with other attacks)
- Combine terminal groups with relatively close probabilities: this improves generation performance, while keeping all generated passwords and without losing much ordering quality
- Remove long grammars and grammars which generate long passwords (longer than 32): this improves generation performance as well

TODO
-
- [ ] explain more