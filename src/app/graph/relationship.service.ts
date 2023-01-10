
import { Injectable } from '@angular/core';
import { Observable, forkJoin, of, mergeMap, catchError } from 'rxjs';
import { map } from 'rxjs/operators';

import { Value, Uri, Literal } from '../rdf/triple';
import { SEE_ALSO, THUMBNAIL, LABEL, IS_A } from '../rdf/defs';

import { QueryService } from '../query/query.service';

import { RelationshipQuery } from '../query/relationship-query';
import { TransformService } from '../transform/transform.service';
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
	private transform : TransformService,
    ) {

    }


    ignoreRelationship(p : Value) {
	if (!p.is_uri()) return false;
	if (p.value() == THUMBNAIL.value()) return true;
	if (p.value() == SEE_ALSO.value()) return true;
	return false;
    }

    getRelationships(id : Uri) : Observable<Relationship[]>{

        return forkJoin({
		in: this.definitions.relationshipKindsIn(id).pipe(
		    this.transform.addConstantColumn(
			"dir", new Literal("in")
		    ),
		),
	        out: this.definitions.relationshipKindsOut(id).pipe(
		    this.transform.addConstantColumn(
			"dir", new Literal("out")
		    ),
		)
	}).pipe(
	    map(rels => {
		return rels.in.data.concat(rels.out.data)
	    }),
	    map(
		rels => rels.filter(
		    rel => !this.ignoreRelationship(rel["pred"])
		)
	    ),
	    mergeMap(
		res => {
		    let ops = res.map(
			    rel => {
			    	return this.definitions.labelQuery(
				    rel["pred"]
				).pipe(
				    map(
					lbl => {
					    rel["name"] = new Literal(lbl);
					    return rel;
					}
				    )
				)
			    }
		    );
		    return forkJoin(ops);
		}
	    ),
	    map(rels => rels.map(
		(row : any) => {
		    let rel = new Relationship();
		    rel.id = row["pred"] as Uri;
		    rel.name = row["name"].value();
		    rel.inward = row["dir"].value() == "in";
		    return rel;
		}
	    )),
	)

    }

    getR(id : Uri){
        return forkJoin({
		in: this.definitions.relationshipKindsIn(id).pipe(
		    this.transform.addConstantColumn(
			"dir", new Literal("in")
		    ),
		),
	        out: this.definitions.relationshipKindsOut(id).pipe(
		    this.transform.addConstantColumn(
			"dir", new Literal("out")
		    ),
		)
	}).pipe(
	    map(rels => {
		return rels.in.data.concat(rels.out.data)
	    }),
	    map(
		rels => rels.filter(
		    rel => !this.ignoreRelationship(rel["pred"])
		)
	    ),
	    mergeMap(
		res => {

		    let ops : any[] = [];

		    for (let row of res) {

			let op = this.definitions.labelQuery(
			    row["pred"]
			).pipe(
			    map(
				lbl => {
				    row["name"] = new Literal(lbl);
				    return row;
				}
			    )
			);
			ops.push(op);
		    }

		    return forkJoin(ops);
		}
	    ),
	)

    }

}

