/*
 *	Usefull method -
 *	for Window Interval calculation
 */


Date.prototype.addDay = function(days){		/* calculate window end from a start date */
    var date = new Date(this.valueOf());
    date.setDate(date.getDate() + days);
    return date;
}


Date.prototype.subDay = function(days){ 	/* calculate window begin from window end date */
    var date = new Date(this.valueOf());
    date.setDate(date.getDate() - days);
    return date;
}

/*
 *	WINDOW INTERVAL SET
 *	begin & end
 */

var day_end = new Date("2022-06-30")			/* Set where interval end */
var day_start = day_end.subDay(2)				/* Set window length (2 days) */

var day_end_str = day_end.toLocaleDateString("Fr")		/* string value of date 
															to use inside aggregation */
var day_start_str = day_start.toLocaleDateString("Fr")


/*
	AGGREGATION
*/

db_name.collection_name.aggregate([					/* set database and collection */
{ $match: 
     {
		"day" : { "$gte" : day_start, "$lte" : day_end}, 
		"payment_method" : "cash",					/* check cash payment only  for the example */
	 }
},

{ $group: 
  {
    "_id" : {
      			"customer_id" : "$customer_id",
      			"customer_type" : "$customer_type"
          	},
	"purchase_count" : { "$sum" : 1 },
	"amount" : { "$sum" : "$amount" }
  }
},

{ $group: 
  {
  		"_id" :{
  		  			"day" : { $concat: [ "[_", day_start_str,"_;_",day_end_str, "_]"] },
  		  			"customer_type" : "$_id.customer_type"
  				},
		"purchase_count" : { "$sum" : "$purchase_count" }, 
		"nb_distinct" : { "$sum" : 1 } ,
		"montant_recharge" : { "$sum" : "$amount" }
	}
}],

{
  cursor: {batchSize: 50},		/* cursor config to avoid data loss on extract_timout  */
  allowDiskUse: true			/* allow disk use for heavy data */
})