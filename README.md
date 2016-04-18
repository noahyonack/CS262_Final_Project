# This is Parallelogram.

Parallelogram is a parallelization package that distributes computationally intensive programs across machines in a distributed system using a distribution model similar to that of the ride-sharing service Uber.

NOTE: Helpful tips on creating a good package structure can be found [here](https://python-packaging.readthedocs.org/en/latest/everything.html)

## To begin using:

Clone and `cd` into the repository. Simply run `python setup.py install` to install the package. Some systems may require `sudo` access.

## How exactly does distribution work?

Uber has a pool of users that can be classified as
either passengers or drivers. When a passenger Sarah requires a lift, she broadcasts a request, which alerts nearby drivers who can then decide to either accept or ignore the request. Drivers who are already carrying passengers oftentimes do not accept the request, and only those drivers who are completely available (and ideally nearby) consider picking up Sarah. Once an Uber driver confirms Sarah’s request, all other available drivers are notified that Sarah no longer needs a ride.

This ride­sharing system will serve as the foundation of our parallelization model. Our library will, on a single machine, partition pieces of the client’s program into parallelizable
chunks.

## What methods does this library expose?
* `p_map(foo, data)`
    * Map a function `foo()` over `data` (of type list). `p_map()` modifies `data` in place
and supplies `foo()` with both the current element of the list and its
respective index.
* `p_filter(foo, data)`
    * Filter `data` (of type list) via a predicate formatted as a function.
* `p_reduce(foo, data)`
    * Reduce `data` (of type list) by continually applying `foo()` to subsequent
	elements of `data`.

## Package Structure

`MANIFEEST.in`: Tells setuptools to include the README when generating source distributions. Otherwise, only Python files will be included.

## How can I run or write tests?

Nose is a Python package that makes running the tests themselves easy. It automatically gets installed when you run `python setup.py install`. To run tests, navigate to the root directory. Then:

`nosetests`

Test files live in `/parallelogram/tests/` and are of the form `test[METHOD].py`