# python-eventide
Unofficial Python client and tooling for the Eventide project (https://eventide-project.org)

Sponsored by [Liveview Technologies](https://lvt.co/).

```python
import asyncio

from eventide import Eventide, Database
from eventide.types import Loop


async def main(loop: Loop):
    db = Database.from_kwargs(
        host='localhost',
        user='message_store',
        password='',
        dbname='message_store',
        loop=loop,
    )
    app = Eventide(db)

    print(await app.get_version())
    print(await app.write_raw_message('accounts', 'checking', {'balance': 5.00}))
    await app.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop))
loop.close()
```
*Output*:
```text
(1, 1, 6)
Message(stream='accounts', type='checking', data={'balance': 5.0}, id=UUID('505395b6-670d-4256-b89d-678b649eeeb1'), metadata=None, position=1623)
```

## Setup

The database

```bash
$ docker run --name message-db -p 5432:5432 -d ethangarofolo/message-db
```

## Testing

Current virtual environment,
```text
$ inv build.test
```

All supported versions,
```text
$ inv build.tox
```

## Benchmarks

*TBD*

## License

```text
MIT License

Copyright (c) 2020 Liveview Technologies

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```