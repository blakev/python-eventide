#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# >>
#   python-eventide, 2020
#   LiveViewTech
# <<

from invoke import task


@task
def lint(c):
    """Sorts, cleans, and formats current code."""
    commands = [
        (
            'Sort imports',
            'isort -j2 .',
        ),
        (
            'Remove unused',
            'autoflake -r --remove-all-unused-imports  --ignore-init-module-imports --remove-unused-variables --exclude scratch.py -i *',
        ), (
            'Fix python format',
            'yapf -r -i -p .',
        ), (
            'Analyze runtime errors',
            'pyflakes eventide/',
        ),
        (
            'Check for misspellings',
            'codespell -q3 --skip=".git,.idea,*.pyc,*.pyo,.mypy*,poetry.lock,build,dist" -I .dictionary',
        )
    ]

    for name, cmd in commands:
        print(' -- ', name)
        c.run(cmd)


@task
def req(c):
    c.run('poetry export -f requirements.txt --without-hashes > requirements.txt')


@task(post=(req,))
def clean(c):
    folders = [
        '.pytest_cache',
        '.tox',
        'build',
        'dist',
        'eventide.egg-info',
    ]

    files = [
        'requirements.txt',
        '.coverage',
    ]

    for file in files + folders:
        c.run(f'rm -rf {file}')


@task
def lock_proj(c):
    c.run('poetry lock -n')


@task
def test(c):
    c.run('python -m pytest -n3 --cov=eventide --cov-report term-missing tests/')


@task
def tox(c):
    c.run('tox -p4')


@task(pre=(
    lock_proj,
    req,
    tox,
))
def sdist(c):
    c.run('python setup.py build sdist')


@task(pre=(sdist,))
def bdist(c):
    c.run('python setup.py build bdist_wheel')
