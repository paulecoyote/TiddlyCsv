# TiddlyCsv
TiddlyCsv is a lightweight, low-ceremony, asynchronous and opinionated csv processing library.

## Features
* You need only include a single file rather than the library.
* Excluding the tests, only references Microsoft.CSharp, System, System.Core.
* Reads from stream are asyncronous.
* Reads values!  Including bare, quoted and cells with embedded quotes and commas.
* Read rows in to plain old data types.
* Read columns as a list within a list of rows.
* Skip rows.
* Passes code analysis with all Microsoft rules on (with a few justifications in source).
* XUnit test project (though could do with some more tests in there).

## Future
* Benchmark against other libraries
* Optimise for speed
* Ensure as much as possible is asynchronous without sacrificing speed

## Contributors
* Paul Evans

## Copyright

Copyright © 2012 Paul Evans and contributors

## License

Licensed under [MIT](http://www.opensource.org/licenses/mit-license.php "Read more about the MIT license form"). Refer to license.txt for more information.
