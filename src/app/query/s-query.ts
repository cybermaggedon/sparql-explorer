
import { Observable } from 'rxjs';

import { Query, QueryResult, QueryEngine } from './query';
import { Triple, Uri, Value } from '../rdf/triple';

export class SQuery implements Query {
    constructor(
	desc : string,
	s : Value,
	limit : number = 100
    ) {
	this.s = s;
	this.desc = desc;
	this.limit = limit;
    }
    s : Value;
    desc : string;
    limit : number = 100;
    description() { return this.desc; }
    hash() : string {
	return "s-q " + this.s.hash() + " " + this.limit;
    }

    
    // FIXME: This is injectable
    // However, not using it against any sensitive data or stores, currently.
    getQueryString() : string {

	let query = "";

	query += "SELECT DISTINCT ?p ?o WHERE {\n";
	query += "  " + this.s.term() + " ?p ?o .\n";

	query += "}\n";
	query += "LIMIT " + this.limit + "\n";

	query = encodeURIComponent(query);
	return "query=" + query + "&output=json";

    };

    run(q : QueryEngine) : Observable<QueryResult> {
	return q.query(this);
    }

}

