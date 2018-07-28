import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { AuthService } from '../core/auth.service';
import { NotificationService } from '../core/notification.service';
import { ConfigureService } from './configure.service';

import { MapComponent } from './map.component';

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

  .container-main {
    height: 85vh;
  }
  `],
  providers: [ConfigureService]
})
export class ConfigureComponent implements OnInit { 
  @ViewChild(MapComponent) map: MapComponent;

  params: any;
  datasets: string[];

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private authService: AuthService,
    private configService: ConfigureService,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.route.queryParams.subscribe(params => {
      this.datasets = (params.dataset_id === undefined) ? [] : params.dataset_id.split(',');

      this.params = { index_node: params.index_node, query: params.query };
    });
  }
}
