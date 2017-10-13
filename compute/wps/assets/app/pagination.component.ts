import { Component, Input, ContentChild, TemplateRef } from '@angular/core';

require('font-awesome-webpack');

export class Header {
  constructor(
    public display: string,
    public key: string
  ) { }
}

@Component({
  selector: 'pagination-table',
  styleUrls: ['./forms.css'],
  template: `
  <div>
    <div class="input-group">
      <span class="input-group-addon"><span class="glyphicon glyphicon-search"></span></span>
      <input type="text" class="form-control" placeholder="Search" (keyup)="search($event)">
    </div>
    <table class="table">
      <thead>
        <tr>
          <th *ngFor="let h of headers" (click)="sort(h.key)">
            <a>
              {{h.display}}
              <i *ngIf="sortKey === h.key && !sortDirection" class="fa fa-sort-asc" aria-hidden="true"></i>
              <i *ngIf="sortKey === h.key && sortDirection" class="fa fa-sort-desc" aria-hidden="true"></i>
            </a>
          </th>
        </tr>
      </thead>
      <tbody>
        <ng-template ngFor [ngForOf]="items | slice:sliceStart:sliceEnd" [ngForTemplate]="content"></ng-template>
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
export class PaginationComponent {
  @Input() headers: Header[];
  @ContentChild(TemplateRef) content: TemplateRef<any>;

  IPP = 20;

  _items: any[];
  _itemsStore: any[];
  sliceStart: number;
  sliceEnd: number;
  page: number;
  maxPage: number;
  sortKey: string;
  sortDirection = true;

  constructor() {
    this.updateSlice(0);
  }

  get items(): any[] {
    return this._items;
  }

  @Input()
  set items(data: any[]) {
    this._itemsStore = this._items = data;

    if (this._items !== null) {
      this.sort(this.headers[0].key);

      this.maxPage = Math.ceil(this._items.length / this.IPP)-1;
    }
  }

  search(event: any) {
    let value = event.target.value;

    this._items = this._itemsStore.filter((item: any) => {
      return this.headers.some((h: Header, i: number, a: Header[]) => {
        return item[h.key].toString().indexOf(value) >= 0;
      });
    });

    this.maxPage = Math.ceil(this._items.length / this.IPP)-1;

    this.sort(this.sortKey);
  }

  sort(key: string) {
    if (this.sortKey != key) {
      this.sortDirection = true;
    }

    this.sortKey = key;

    this.items.sort((a: any, b: any) => {
      if (a[key] > b[key]) {
        return 1;
      } else if (a[key] < b[key]) {
        return -1;
      } else {
        return 0;
      }
    });

    if (this.sortDirection) {
      this.sortDirection = false;
    } else {
      this.sortDirection = true;

      this.items.reverse();
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
