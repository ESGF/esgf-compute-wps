import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { NotificationService } from '../core/notification.service';

@Component({
  templateUrl: './configure.component.html',
  styles: [`
  .pane {
    padding: 1em;
  }

  .select-spacer {
    margin-bottom: 10px;
  }

  .list-item-axis {
    margin: 5px 0px 5px 0px;
  }
  `],
  providers: []
})
export class ConfigureComponent implements OnInit { 
  params: any;
  datasets: string[];

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.route.queryParams.subscribe(params => {
      this.datasets = (params.dataset_id === undefined) ? [] : params.dataset_id.split(',');

      this.params = { index_node: params.index_node, query: params.query };
    });
  }
}
