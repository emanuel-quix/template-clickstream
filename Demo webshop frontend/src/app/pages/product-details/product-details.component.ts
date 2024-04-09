import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { Observable } from 'rxjs';
import { PRODUCTS } from 'src/app/constants/products';
import { Product } from 'src/app/models/product';
import { DataService } from './../../services/data.service';
import { User } from 'src/app/models/user';
import { WssSendService } from 'src/app/services/wss-send.service';
import { EnvironmentVariableService } from 'src/app/services/environment-variable.service';


@Component({
  templateUrl: './product-details.component.html',
  styleUrls: ['./product-details.component.scss']
})
export class ProductDetailsComponent implements OnInit {
  product: Product | undefined;
  isMainSidenavOpen$: Observable<boolean>;

  constructor(
    private route: ActivatedRoute,
    private dataService: DataService,
    private wssSendService: WssSendService,
    private environmentVariables: EnvironmentVariableService
  ) { }

  ngOnInit(): void {
    const productId = this.route.snapshot.paramMap.get('id');
    this.product = PRODUCTS.find((f) => f.id === productId);

    this.sendData();

    this.isMainSidenavOpen$ = this.dataService.isSidenavOpen$.asObservable();
  }

  sendData(): void {
    if (!this.product) return;

    console.log("Sending data to wss server...")
    const user: User = this.dataService.user;
    const productId = this.product.id;

    this.dataService.getIpAddress().subscribe((ip) => {
      var data = {
        'userId': user.userId,
        'age': user.age.toString(),
        'gender': user.gender,
        'ip': ip,
        'userAgent': navigator.userAgent,
        'productId': productId,
      }
      this.wssSendService.connectAndSend(this.environmentVariables.clickTopic, user.userId, data);
    });
  }

  clearSelection(): void {
    this.dataService.categorySelection = [];
  }

}
