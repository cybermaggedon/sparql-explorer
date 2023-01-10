
import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { timer, forkJoin, of } from 'rxjs';
import { map, mergeMap } from 'rxjs/operators';

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

    relationships : { [key : string] : any } = {};

    constructor(
	private route : ActivatedRoute,
	private properties : PropertiesService,
	private relationship : RelationshipService,
	private definitions : DefinitionsService,
	private transform : TransformService,
    ) {

    }

    ngOnInit(): void {

	this.route.queryParams.subscribe(
	    params => {
		if (params["node"]) {
		    timer(1000).subscribe(
			() => this.query(params["node"])
		    );
		}
	    }
	);

    }

    query(id : string) {

	let n = new Node();
	n.id = id;

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
	    }
	);

	forkJoin({
	    i: this.definitions.relationshipKindsIn(new Uri(id)).pipe(
		this.transform.addConstantColumn("dir", new Literal("in"))
	    ),
	    o: this.definitions.relationshipKindsOut(new Uri(id)).pipe(
		this.transform.addConstantColumn("dir", new Literal("out"))
	    )		
	}).pipe(
	    map(
		rels => {
		    return rels.i.data.concat(rels.o.data);
		}
	    ),
	    mergeMap(
		rels => {
		    let obs : any = {};
		    for (let rel of rels) {
			let pred = rel["pred"];
			let ob = this.definitions.relationshipsOutwards(
			    new Uri(id), pred
			).pipe(
			    this.definitions.joinLabel("o", "olabel"),
			    map(res => {
				return {
				    inward: rel["dir"].value() == "in",
				    targets: res.data.map(
					row => {
					    return {
						id: row["o"],
						label: row["olabel"],
					    };
					}
				    ),
				};
			    })
			);
			obs[pred.value()] = ob;
		    }
		    return forkJoin(obs);
		}
	    ),
	).subscribe(
	    (res : any) => {
		console.log(res);
		this.relationships = res;
	    }
	);

    }

}

