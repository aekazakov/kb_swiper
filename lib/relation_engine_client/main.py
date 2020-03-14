import json
import requests
from typing import Optional, List, Dict, Union

from .exceptions import REServerError, RERequestError, RENotFound

_QUERY_METHOD = 'POST'
_QUERY_ENDPOINT = '/api/v1/query_results'
_SAVE_METHOD = 'PUT'
_SAVE_ENDPOINT = '/api/v1/documents'


class REClient:

    def __init__(self, api_url: str, token: str = None):
        self.api_url = api_url
        self.token = token
        # Type check the constructor parameters
        if not self.api_url or not isinstance(self.api_url, str):
            raise TypeError("The Relation Engine API URL was not provided.")
        # Remove any trailing slash in the API URL so we can append paths
        self.api_url = self.api_url.strip('/')

    def admin_query(self, query: str, bind_vars: dict, raise_not_found=False):
        """
        Run an ad-hoc query using admin privs.
        Params:
            query - string - AQL query to execute
            bind_vars - dict - JSON serializable bind variables for the query
            raise_not_found - bool - Whether to raise an error if there are zero results. Defaults to False
        Exceptions raised:
            RERequestError - 400-499 error from the RE API
            REServerError - 500+ error from the RE API
            RENotFound - raised when raise_not_found is True and there are 0 results
        """
        # Type-check the parameters
        if not isinstance(query, str):
            raise TypeError("`query` argument must be a str")
        if not isinstance(bind_vars, dict):
            raise TypeError("`bind_vars` argument must be a dict")
        if not isinstance(raise_not_found, bool):
            raise TypeError("`raise_not_found` argument must be a bool")
        # Construct and execute the request
        req_body = dict(bind_vars)
        req_body['query'] = query
        url = str(self.api_url) + _QUERY_ENDPOINT
        resp = self._make_request(
            method=_QUERY_METHOD,
            url=url,
            data=json.dumps(req_body),
            params={},
            raise_not_found=raise_not_found)
        return resp

    def stored_query(self, stored_query: str, bind_vars: dict, raise_not_found=False):
        """
        Run a stored query.
        Params:
            stored_query - string - name of the stored query to execute
            bind_vars - JSON serializable - bind variables for the query (JSON serializable)
            raise_not_found - bool - Whether to raise an error if there are zero results. Defaults to False
        Exceptions raised:
            RERequestError - 400-499 from the RE API (client error)
            REServerError - 500+ error from the RE API
            RENotFound - raised when raise_not_found is True and there are 0 results
        """
        # Type-check the parameters
        if not isinstance(stored_query, str):
            raise TypeError("`stored_query` argument must be a str")
        if not isinstance(bind_vars, dict):
            raise TypeError("`bind_vars` argument must be a dict")
        if not isinstance(raise_not_found, bool):
            raise TypeError("`raise_not_found` argument must be a bool`")
        # Construct and execute the request
        req_body = dict(bind_vars)
        url = str(self.api_url) + _QUERY_ENDPOINT
        return self._make_request(
            method=_QUERY_METHOD,
            url=url,
            data=json.dumps(req_body),
            params={'stored_query': stored_query},
            raise_not_found=raise_not_found)

    def save_docs(
            self,
            coll: str,
            docs: Union[Dict, List[Dict]],
            on_duplicate: Optional[str] = None,
            display_errors=False):
        """
        Save documents to a collection in the relation engine.
        Requires an auth token with RE admin privileges.
        Params:
            coll - str - collection name to save to
            docs - a single dict or list of dicts - json-serializable documents to save
            on_duplicate - str (defaults to 'error') - what to do when a provided document
                already exists in the collection. See options here:
                https://github.com/kbase/relation_engine_api#put-apiv1documents
            display_errors - bool (defaults to False) - whether to respond with
                document save errors (the response will give you an error for every
                document that failed to save).
        Exceptions raised:
            RERequestError - 400-499 from the RE API (client error)
            REServerError - 500+ error from the RE API
        """
        if isinstance(docs, dict):
            docs = [docs]
        if not docs:
            raise TypeError("No documents provided to save")
        if not isinstance(docs, list):
            raise TypeError("`docs` argument must be a list")
        if on_duplicate and not isinstance(on_duplicate, str):
            raise TypeError("`on_duplicate` argument must bea str")
        if not isinstance(display_errors, bool):
            raise TypeError("`display_errors` argument must be a bool")
        params = {'collection': coll}
        if display_errors:
            params['display_errors'] = '1'
        params['on_duplicate'] = on_duplicate or 'error'
        req_body = '\n'.join(json.dumps(d) for d in docs)
        url = str(self.api_url) + _SAVE_ENDPOINT
        return self._make_request(
            method=_SAVE_METHOD,
            url=url,
            data=req_body,
            params=params,
            raise_not_found=False)

    def _make_request(self, method, url, data, params, raise_not_found):
        """
        Internal utility to make a generic request to the RE API and handle the
        response.
        """
        headers = {}
        if self.token:
            headers['Authorization'] = self.token
        resp = requests.request(method=method, url=url, data=data, params=params, headers=headers)
        if resp.status_code >= 500:
            # Server error
            raise REServerError(resp)
        elif resp.status_code >= 400 and resp.status_code < 500:
            # Client error
            raise RERequestError(resp)
        elif not resp.ok:
            raise RuntimeError(
                f"Unknown RE API error:\nURL: {resp.url}\nMethod: {method}\n{resp.text}")
        resp_json = resp.json()
        if raise_not_found and not len(resp_json['results']):
            # Results were required to be non-empty
            raise RENotFound(req_body=data, req_params=params)
        return resp_json
