
import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { ReactiveFormsModule } from '@angular/forms';

import { ButtonModule } from 'primeng/button';
import { TableModule } from 'primeng/table';
import { InputTextModule } from 'primeng/inputtext';
import { DialogModule } from 'primeng/dialog'; // Unused?
import { TabViewModule } from 'primeng/tabview';
import { ToolbarModule } from 'primeng/toolbar';
import { PanelModule } from 'primeng/panel';
import { CardModule } from 'primeng/card';
import { TagModule } from 'primeng/tag';
import { ImageModule } from 'primeng/image';
import { InputTextareaModule } from 'primeng/inputtextarea';
import { DataViewModule } from 'primeng/dataview';

import { GraphViewerComponent } from './graph-viewer/graph-viewer.component';
import { GraphComponent } from './graph/graph.component';
import { SearchComponent } from './search/search.component';
import { InfoComponent } from './info/info.component';
import { NodeDetailsComponent } from './node-details/node-details.component';
import { NodeDialogComponent } from './node-dialog/node-dialog.component';
import { SearchDialogComponent } from './search-dialog/search-dialog.component';
import { SchemaDialogComponent } from './schema-dialog/schema-dialog.component';
import { SchemaComponent } from './schema/schema.component';
import { DatasetsDialogComponent } from './datasets-dialog/datasets-dialog.component';
import { DatasetsComponent } from './datasets/datasets.component';
import { InfoDialogComponent } from './info-dialog/info-dialog.component';
import { AboutDialogComponent } from './about-dialog/about-dialog.component';
import { AccordionModule } from 'primeng/accordion';
import { ImportDialogComponent } from './import-dialog/import-dialog.component';
import { ExportDialogComponent } from './export-dialog/export-dialog.component';
import { GalleryDialogComponent } from './gallery-dialog/gallery-dialog.component';
import { GalleryComponent } from './gallery/gallery.component';

@NgModule({
    declarations: [
	GraphViewerComponent,
	GraphComponent,
	SearchComponent,
	InfoComponent,
        NodeDetailsComponent,
        NodeDialogComponent,
        SearchDialogComponent,
        SchemaDialogComponent,
        SchemaComponent,
        DatasetsDialogComponent,
        DatasetsComponent,
        InfoDialogComponent,
        AboutDialogComponent,
        ImportDialogComponent,
        ExportDialogComponent,
        GalleryDialogComponent,
        GalleryComponent,
    ],
    imports: [

	BrowserModule,
	FormsModule,
	ReactiveFormsModule,

	ButtonModule,
	TableModule,
	InputTextModule,
	DialogModule,
	TabViewModule,
	ToolbarModule,
	PanelModule,
	CardModule,
	ImageModule,
	TagModule,
	AccordionModule,
	InputTextareaModule,
	DataViewModule,

    ],
    exports: [
	GraphViewerComponent,
    ],
    providers: [],
    bootstrap: [
    ]
})
export class GraphViewerModule { }

