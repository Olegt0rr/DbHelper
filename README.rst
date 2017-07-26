== DbHelper ==
Module that helps you to save time and lines

It runs all queries, fetches, connects, disconnects, commits etc. within itself :)
Just use it like:

.. code:: python
	from db import DbHelper
	db = DbHelper()

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
