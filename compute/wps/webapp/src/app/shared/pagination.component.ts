import { Component, Input, ContentChild, TemplateRef } from '@angular/core';

require('font-awesome-webpack');

export class Header {
  constructor(
    public display: string,
    public key: string
  ) { }
}

@Component({
  selector: 'pagination',
  styleUrls: ['../forms.css'],
  template: `
  <div>
    <div>
      <ng-template ngFor [ngForOf]="data | slice:sliceStart:sliceEnd" [ngForTemplate]="content"></ng-template>
    </div>
    <nav aria-label="Page navigation">
      <ul class="pager">
        <li [class.disabled]="page === 0" (click)="changePage(-1)"><a>Previous</a></li>
        <li [class.disabled]="page === maxPage" (click)="changePage(1)"><a>Next</a></li>
      </ul>
    </nav>
  </div>
  `
})
export class PaginationComponent {
  @ContentChild(TemplateRef) content: TemplateRef<any>;

  IPP = 20;

  data: any[] = [];
  dataStore: any[];
  sliceStart: number;
  sliceEnd: number;
  page: number;
  maxPage: number;

  constructor() {
    this.updateSlice(0);
  }

  get items(): any[] {
    return this.data;
  }

  @Input()
  set items(data: any[]) {
    this.dataStore = this.data = data;

    if (this.data !== null) {
      this.maxPage = Math.max(Math.ceil(this.data.length / this.IPP)-1, 0);
    }
  }

  updateSlice(page: number) {
    this.page = page;

    this.sliceStart = this.page * this.IPP;

    this.sliceEnd = this.IPP + this.sliceStart;
  }

  changePage(delta: number) {
    let newPage = this.page + delta;

    if (newPage >= 0 && newPage <= this.maxPage) {
      this.updateSlice(newPage);
    }
  }
}

@Component({
  selector: 'pagination-table',
  styleUrls: ['../forms.css'],
  template: `
  <div>
    <div *ngIf="searchEnabled" class="input-group">
      <span class="input-group-addon"><span class="glyphicon glyphicon-search"></span></span>
      <input type="text" class="form-control" placeholder="Search" (keyup)="search($event)">
    </div>
    <table class="table">
      <thead *ngIf="headers !== undefined">
        <tr>
          <th *ngFor="let h of headers" (click)="sort(h.key)">
            <a>
              {{h.display}}
              <i *ngIf="sortKey === h.key && sortAsc" class="fa fa-sort-asc" aria-hidden="true"></i>
              <i *ngIf="sortKey === h.key && !sortAsc" class="fa fa-sort-desc" aria-hidden="true"></i>
            </a>
          </th>
        </tr>
      </thead>
      <tbody>
        <ng-template ngFor [ngForOf]="data | slice:sliceStart:sliceEnd" [ngForTemplate]="content"></ng-template>
      </tbody>
    </table>
    <nav aria-label="Page navigation">
      <ul class="pager">
        <li [class.disabled]="page === 0" (click)="changePage(-1)"><a>Previous</a></li>
        <li [class.disabled]="page === maxPage" (click)="changePage(1)"><a>Next</a></li>
      </ul>
    </nav>
  </div>
  `
})
export class PaginationTableComponent extends PaginationComponent {
  @Input() headers: Header[];
  @Input('search') searchEnabled = true;

  sortKey: string;
  sortAsc = true;

  @Input()
  set items(data: any[]) {
    this.dataStore = this.data = data;

    if (this.data !== null) {
      if (this.headers !== undefined) {
        this.sort(this.headers[0].key);
      }

      this.maxPage = Math.max(Math.ceil(this.data.length / this.IPP)-1, 0);
    }
  }

  search(event: any) {
    let value = event.target.value;

    this.data = this.dataStore.filter((item: any) => {
      return this.headers.some((h: Header, i: number, a: Header[]) => {
        return item[h.key].toString().indexOf(value) >= 0;
      });
    });

    this.maxPage = Math.max(Math.ceil(this.data.length / this.IPP)-1, 0);

    this.sort(this.sortKey);
  }

  sort(key: string) {
    if (this.data === undefined) return;

    this.sortKey = key;

    this.sortAsc = !this.sortAsc;

    this.data.sort((a: any, b: any) => {
      if (a[key] > b[key]) {
        return 1;
      } else if (a[key] < b[key]) {
        return -1;
      } else {
        return 0;
      }
    });

    if (!this.sortAsc) {
      this.data.reverse();
    }
  }
}
