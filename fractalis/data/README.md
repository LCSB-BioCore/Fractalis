### About

This page contains instructions on how data are loaded into Fractalis.

### General

First, it is important to understand that Fractalis, unlike other analytical 
platforms, does not have a persistent database in the traditional sense.
Data are "imported" on-demand into the analysis cache. Whether that happens via
REST API, some sort of data stream, or file import is entirely up to the
MicroETL.

### MicroETLs

MicroETLs in Fractalis are submittable jobs that are responsible for the data
(E)xtraction from the target service, the (T)ransformation into an internal
standard format, and the (L)oading into the analysis cache. MicroETLs can be
very simple or very complex. It highly depends on how easy it is, to extract
data into a workable format, but generally it should only take a few hours to
have some basic implementation. The [Ada Integer ETL](etls/ada/etl_integer.py)
is a good example for a simple MicroETL.


### Implementation

There are very few restrictions on how a MicroETL should look like. It is
entirely up to you how to decide how to extract data from the service you want
to support. If your service offers a REST API, we recommend using the Python
requests module. Nothing stops you from directly accessing the database or some
files, though. Inspiration can be found [here](etls).

The only real requirement is, that your MicroETL must inherit the 
[ETL Class](etl.py).
This class is responsible for making your MicroETL a submittable celery job and 
that your MicroETL produces the correct internal format, among other things.

You don't have to understand the ETL class in order to inherit from it. It is
designed in a way that you should always get a readable error if you do something
wrong. It won't hurt to have a look, though.

### ETL Variables

`descriptor` (dict), `handler` (str), `server` (str), `auth` (dict).

It is up to the front-end to decide what they contain. They are used like this:
- Fractalis decides which MicroETL group/handler to use based on the `handler` 
(e.g. `ada`)
- The MicroETLs in that group decide whether they can handle the request based 
on the information in `descriptor` (e.g. `{'data-type': 'image', ...}`)
- The data are extracted from the `server` (e.g. `https://localhost`) 
- The ETL authenticates itself using authentication `auth` (e.g. `{'token': 123345}`)
- The ETL decides what to download from the server based on the `descriptor` (e.g. `{..., 'field': 'Age'}`)

### Internal Formats

Fractalis technically supports all formats. Yes, all. On a very basic level, Fractalis
is a distributed job framework with MicroETLs that executed Python/R scripts on
extracted data. Nothing stops you from loading brain image data, genomic data, or 
financial data into Fractalis and code a visualisation for it. It doesn't mean you 
should do that, though. There is two factors that should be taken into account:
1. **The data size.** It wouldn't be a good idea to move half a terabyte of 
genomic data into Fractalis via REST API.
Instead you might want to consider connecting analyses or ETLs with other systems
like [Hail](https://github.com/hail-is/hail) to merge analyses results or data 
from different sources into a single visualisation managed by Fractalis.
2. **How much time you want to spend coding your own visualisation.** You can of
course import financial or wheather data into Fractalis, but you will likely not
profit much from the existing analysis scripts or visualisations. Fractalis focus
is **explorative** analysis in the field of **translational research**, so you should
consider this, when thinking about adding a new format.

TL;DR: To see which formats are currently used and how they are defined, please
look at the [integrity check modules](integrity).
If you want to add a new data type, this is the only place you have to touch.

### FAQ

> Why is there no `load` method in the MicroETLs?

- There is, but you don't have to add it yourself. That's because the format
returned by `transform` is a internal standard format (after passing integrity checks),
so the loading step is the same for all MicroETLs of that type. 
