
import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { CommandService, Direction } from '../command.service';
import { SelectionService } from '../selection.service';
import { GraphService } from '../graph.service';
import { QueryService, Query } from '../../query.service';

@Component({
  selector: 'controls',
  templateUrl: './controls.component.html',
  styleUrls: ['./controls.component.scss']
})
export class ControlsComponent implements OnInit {

    selection : string = "";

    info1 : string = "";
    info2 : string = "";

    constructor(
	private command : CommandService,
	private select : SelectionService,
	private graph : GraphService,
	private router : Router,
	private query : QueryService,
    ) {

	/*
	this.select.selectEvents().subscribe(
	    ev => this.selection = ev.id
	);

	this.select.unselectEvents().subscribe(
	    () => this.selection = ""
	);
*/
	this.graph.nodeSelectEvents().subscribe(
	    ev => this.selection = ev.id
	);

	this.graph.nodeDeselectEvents().subscribe(
	    () => this.selection = ""
	);

	this.query.progress().subscribe(

	    (res : Set<Query>) => {

		let a = Array.from(res.values());

		if (a.length > 0)
		    this.info1 = a[0].desc + " ...";
		else
		    this.info1 = "";

		if (a.length > 1)
		    this.info2 = a[1].desc + " ...";
		else
		    this.info2 = "";

	    }

	);

    }

    ngOnInit(): void {
    }

    expandIn() {
	if (this.selection)
	    this.command.expand(Direction.IN, this.selection);
    }

    expandOut() {
	if (this.selection)
	    this.command.expand(Direction.OUT, this.selection);
    }

    restart() {
	this.router.navigate(
	    ["/"]
	);
    }

    recentre() {
	if (this.selection)
	    this.command.recentre(this.selection);
    }

    schema() {
	this.command.showSchema();
    }

}
