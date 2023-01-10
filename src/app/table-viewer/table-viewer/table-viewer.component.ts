
import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { timer, forkJoin } from 'rxjs';
import { map } from 'rxjs/operators';

import { PropertiesService } from '../../graph/properties.service';
import { Node } from '../../graph/graph';
import { Uri, Literal } from '../../rdf/triple';
import { RelationshipService } from '../../graph/relationship.service';
import { DefinitionsService } from '../../query/definitions.service';
import { TransformService } from '../../transform/transform.service';

@Component({
    selector: 'table-viewer',
    templateUrl: './table-viewer.component.html',
    styleUrls: ['./table-viewer.component.scss']
})
export class TableViewerComponent implements OnInit {

    node : string = "";

    detail : { key: string, value: string }[] = [];

    constructor(
	private route : ActivatedRoute,
	private properties : PropertiesService,
	private relationship : RelationshipService,
	private definitions : DefinitionsService,
	private transform : TransformService,
    ) {

	this.route.queryParams.subscribe(
	    params => {
		if (params["node"]) {
		    timer(1).subscribe(
			() => this.query(params["node"])
		    );
		}
	    }
	);

    }

    rows : any[] = [];
    vars : string[] = [];

    ngOnInit(): void {
    }

    query(id : string) {

	let n = new Node();
	n.id = id;
/*
	this.properties.getProperties(n).subscribe(
	    props => {
		this.detail = props.properties.map(
		    prop => {
			return {
			    key: prop.key,
			    value: prop.value.value(),
			}
		    }
		);
		console.log(this.detail);
	    }
	);
*/
	/*
	this.relationship.getRelationships(new Uri(id)).subscribe(
	    (res : any) => {
		console.log(res)
	    }
	    );
	*/

	forkJoin(
	    {
		i: this.definitions.relationshipKindsIn(new Uri(id)).pipe(
		    this.transform.addConstantColumn(
			"dir", new Literal("in")
		    ),
		),
		o: this.definitions.relationshipKindsOut(new Uri(id)).pipe(
		    this.transform.addConstantColumn(
			"dir", new Literal("out")
		    ),
		),
	    }
	).pipe(
	    map(
		res => {
		    return {
			vars: res.i.vars,
			data: res.i.data.concat(res.o.data),
		    };
		}
	    ),
	    this.definitions.joinLabel("pred", "name"),
	).subscribe(
	    (res : any) => {
		console.log(res);
	    }
	);

    }

}

