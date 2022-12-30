
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import {
    BehaviorSubject, Subject, Observable, forkJoin, Subscriber, of,
    combineLatest, mergeMap
} from 'rxjs';
import { map } from 'rxjs/operators';

import { Triple, Value, Uri, Literal } from '../query/triple';
import { QueryService } from '../query/query.service';

import { TripleQuery } from '../query/triple-query';
import { SQuery } from '../query/s-query';
import { QueryResult, Row } from '../query/query';
import { RelationshipQuery } from '../query/relationship-query';
import { TextSearchQuery } from '../query/text-search-query';
import { LabelQuery } from '../query/label-query';
import { GraphService } from './graph.service';
import { TransformService } from '../query/transform.service';
import { Node } from './graph';
import { EventService } from './event.service';

import { SEE_ALSO, THUMBNAIL, LABEL, IS_A } from '../rdf';

export type PropertyMap = { [key : string] : Value };

export class Properties {
    properties : PropertyMap = {};
};

@Injectable({
    providedIn: 'root'
})
export class PropertiesService {

    constructor(
	private query : QueryService,
	private graph : GraphService,
	private transform : TransformService,
	private events : EventService,
    ) {

	this.events.nodeSelectEvents().subscribe(
	    (ev : any) => {
		this.getProperties(ev.node);
	    }
	);

    }

    propertyEdges = 25;

    private propertiesSubject = new Subject<Properties>;

    propertiesEvents() { return this.propertiesSubject; }

    getProperties(node : Node) {
	new SQuery(
	    "Fetch " + node.id,
	    new Uri(node.id),
	    this.propertyEdges,
	).run(
	    this.query
	).pipe(
	    this.transform.filterRelationships(),
	    this.mapToProperties(),
	).subscribe(
	    res => {
		let prop = new Properties();
		for (let row of res) {
		    prop.properties[row["p"].value()] = row["o"];
		}
		this.propertiesSubject.next(prop);
	    }
	);
    }

    mapToProperty(row : Row) {
	return new Observable<Row>(
	    sub => {
		
		let p = row["p"] as Uri;
		let o = row["o"];
		    
		if (p.is_uri() && (p.value() == LABEL.value())) {
			
		    // Label
		    sub.next({
			p: new Literal("label"),
			o: o
		    });
		    sub.complete();

		} else if (p.is_uri() && (p.value() == THUMBNAIL.value())) {
		    
		    // thumbnail
		    sub.next({p: new Literal("thumbnail"), o: o});
		    sub.complete();

		} else if (p.is_uri() && (p.value() == SEE_ALSO.value())) {

		    // link
		    sub.next({
			p: new Literal("link"), o: o
		    });
		    sub.complete();
		    
		} else if (p == IS_A) {

		    this.mapToClassLabel(o as Uri, sub);

		} else if (o.is_uri()) {

		    sub.next({});
		    sub.complete();

		} else {

		    // 'o' is a literal, just need the
		    // human-readable property name.
		    this.mapToLiteral(p, o as Uri, sub);

		}
	    }

	);
    }

    mapToProperties() {
	return mergeMap((qr : QueryResult) => {

	    let obs : any[] = [];
	    
	    for (let row of qr.data) {
		obs.push(this.mapToProperty(row));
	    }

	    return forkJoin(obs);
	    
	});
    }

    mapToLiteral(p : Uri, o : Value, sub : Subscriber<Row>) {

	new LabelQuery("Label " + p, p).run(
	    this.query
	).subscribe(
	    lbl => {
		if (lbl) {
		    sub.next({p: new Literal(lbl), o: o});
		    sub.complete();
		    return;
		} else {
		    sub.next({p: new Literal(this.transform.makeLabel(p)), o: o});
		    sub.complete();
		    return;
		}
	    }
	    
	)
    }

    mapToClassLabel(id : Uri, sub : Subscriber<Row>) {

	// IS_A relationship, work out the class name

	new LabelQuery("Label " + id, id,).run(
	    this.query
	).subscribe(
	    lbl => {
		if (lbl) {
		    sub.next({
			p: new Literal("class"), o: new Literal(lbl)
		    });
		    sub.complete();
		    return;
		} else {
		    sub.next(
			{
			    p: new Literal("class"),
			    o: new Literal(this.transform.makeLabel(id))
			}
		    );
		    sub.complete();
		    return;
		}
		
	    }

	);

    }

    getProps(node : Node) : Observable<any> {
	
	return new TripleQuery(
	    "Fetch " + node.id,
	    new Uri(node.id),
	    undefined,
	    undefined,
	    this.propertyEdges,
	).run(
	    this.query
	).pipe(
	    map(
		(x : QueryResult) => x.data.map(
		    (y : any) => [y.p, y.o]
		)
	    ),
	    this.transform.mapAddLabel(0),
	    this.transform.mapAddLabel(1),
	    map(
		(x : Value[][]) => {
		    let res : { key : string, value : string }[] = [];
		    for (let row of x) {
			res.push({key: row[2].value(), value: row[3].value()});
		    }
		    return res;
		}
	    ),
	);
    }

}

