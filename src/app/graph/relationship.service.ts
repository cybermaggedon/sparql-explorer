
import { Injectable } from '@angular/core';
import { Observable, forkJoin, combineLatest } from 'rxjs';
import { map } from 'rxjs/operators';

import { Value, Uri } from '../rdf/triple';
import { SEE_ALSO, THUMBNAIL, LABEL, IS_A } from '../rdf/defs';

import { QueryService } from '../query/query.service';

import { RelationshipQuery } from '../query/relationship-query';
import { DefinitionsService } from '../query/definitions.service';
import { GraphService} from './graph.service';
import { Node, Edge, Relationship } from './graph';


@Injectable({
    providedIn: 'root'
})
export class RelationshipService {

    constructor(
	private query : QueryService,
	private graph : GraphService,
	private definitions : DefinitionsService,
    ) {

    }

    ignoreRelationship(p : Value) {
	if (!p.is_uri()) return false;
	if (p.value() == THUMBNAIL.value()) return true;
	if (p.value() == SEE_ALSO.value()) return true;
	return false;
    }

    getRelationshipsIn(id : string) : Observable<Value[]> {
	return this.definitions.relationshipKindsIn(id).pipe(
	    map(
		(v : Value[]) => v.filter(
		    v => !this.ignoreRelationship(v)
		)
	    )
	);

    }

    getRelationshipsOut(id : string) : Observable<Value[]> {

	return this.definitions.relationshipKindsOut(id).pipe(
	    map(
		(v : Value[]) => v.filter(
		    v => !this.ignoreRelationship(v)
		)
	    )
	);

    }

    getRelationshipPreds(id : string) : Observable<Relationship[]>{

	return new Observable<any>(

	    sub => {

		let inw = this.getRelationshipsIn(id);

		let outw = this.getRelationshipsOut(id);
	    
		// combineLatest maybe?

		forkJoin({
		    "in": inw,
		    "out": outw,
		}).pipe(
		    map(
			(res : { [key : string] : Value[] }) => {
			
			    let exps : Relationship[] = [];
			    
			    for (let i of res["in"]) {
				let exp = new Relationship();

				// Assumption
				exp.id = i as Uri;
				exp.inward = true;
				exps.push(exp);
			    }

			    for (let i of res["out"]) {
				let exp = new Relationship();

				// Assumption
				exp.id = i as Uri;
				exp.inward = false;
				exps.push(exp);
			    }

			    return exps;

			}
		    )
		).subscribe(
		    exps => sub.next(exps)
		);

	    }
	);

    }

    getRelationships(id : string) {

	return new Observable<Relationship[]>(
	    sub => {

		this.getRelationshipPreds(id).subscribe(
		    exps => {
			    
			let todo : any[] = [];
			    
			for(let exp of exps) {

			    todo.push(this.graph.getLabel(exp.id).pipe(
				map(
				    (label : string) => {
					let e = new Relationship();
					e.id = exp.id;
					e.name = label;
					e.inward = exp.inward;
					return e;
				    }
				)
			    ));

			}

			combineLatest(todo).subscribe(
			    ev => {
				sub.next(ev);
			    }
			);

		    }
		);

	    }
	);

    }

}

