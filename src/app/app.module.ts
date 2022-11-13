import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { QueryTableComponent } from './query-table/query-table.component';
import { QueryGraphComponent } from './query-graph/query-graph.component';
import { GraphComponent } from './graph/graph.component';
import { TechExplorerComponent } from './tech-explorer/tech-explorer.component';

@NgModule({
  declarations: [
    AppComponent,
    QueryTableComponent,
    QueryGraphComponent,
    GraphComponent,
    TechExplorerComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
