-------
Db Helper
-------
Module that helps you to save time and lines
It runs all queries, fetches, connects, disconnects, commits etc. within itself :)

-------
Using
-------

Just insert:

.. code:: python

    from db import DbHelper
    db = DbHelper()

at the beginning of your script.

You can also use logs in your application by calling ``logging.getLogger()`` and setting the log level you want:

.. code:: python

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)


SELECT example - 1 (clear query):

.. code:: python

    query = "SELECT * FROM table"
    result = db.fetchone(query)

SELECT example - 2 (query with data):

.. code:: python

    query = "SELECT * FROM table WHERE some_column = %s"
    data = "your param"
    result = db.fetchone(query, data)

If you need to fetch some results from same query use this case on first query:

.. code:: python

    result = db.fetchone(query, data, open_cursor=True)
	
and this case on the next one:

.. code:: python

    result = db.fetchone(query, data, next_result=True)
	
don' forget to close_cursor after all

.. code:: python

    db.close_cursor()
