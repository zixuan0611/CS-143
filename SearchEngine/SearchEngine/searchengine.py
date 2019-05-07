#!/usr/bin/python3

from flask import Flask, render_template, request

import search

application = app = Flask(__name__)
app.debug = True

@app.route('/search', methods=["GET"])
def dosearch():
    query = request.args['query']
    qtype = request.args['query_type']
    n_page = int(request.args['n_page'])
    n_row = 0
    search_results = None

    """
    TODO:
    Use request.args to extract other information
    you may need for pagination.
    """
    if 'previous' in request.args:
        n_page, n_row, search_results = search.pagination(n_page-1)
    elif 'next' in request.args:
        n_page, n_row, search_results = search.pagination(n_page+1)
    else:
        n_page=1
        n_row, search_results = search.search(query, qtype, n_page)


    return render_template('results.html',
            query=query,
            results=len(search_results),
            search_results=search_results,
            n_row=n_row,
            n_page=n_page)

@app.route("/", methods=["GET"])
def index():
    if request.method == "GET":
        pass
    return render_template('index.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0')
