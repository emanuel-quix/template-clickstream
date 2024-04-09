export class Offer {

    constructor(data: any) {
        if (data.Value !== undefined){
            this.Offer = data.Value;
            this.IsValid = true;
        }
        else{
            // throw "Offer data is malformed";
        }
    }

	Offer: string;
    IsValid: boolean = false;
}
