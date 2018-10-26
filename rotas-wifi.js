db.reading.aggregate([
	{$match: {address: "d0040145800a"}},
	{$project: {address: "$address", 
				dia: {$dateToString: {date: {$toDate: {$subtract: ["$transaction_date", 10800000]}}, format: "%Y-%m-%d"}},
				hora: {$hour: {$toDate: {$subtract: ["$transaction_date", 10800000]}}},
				minuto: {$minute:{$toDate: "$transaction_date"}},
				receiver: "$receiver_address",
				rssi: "$rssi"
	}},
	{$sort: {"address": 1, "dia": 1, "hora": 1, "minuto": 1}},
	{$group: {
		_id: {
			address: "$address", 
			dia: "$dia", 
			hora: "$hora",
			minuto: "$minuto"
		},
		all_receivers: {$push:{$cond:[{$gte: ["$rssi", -70]}, "$receiver", "000"]}}
	}},
	{$project: {
		rota: {$reduce: {
			input: {$filter: {input: "$all_receivers", as: "mac", cond: {$not:{$eq:["$$mac", "000"]}}}} , 
			initialValue: {last: "", res: []}, 
			in: {res: {$concatArrays:[
					"$$value.res",
					{$cond:[
						{$eq: ["$$value.last", "$$this"]},
						[], 
						["$$this"]
					]}
				]}, 
				last:"$$this"
			}
		}}
	}},
	{$group: {
		_id: { address: "$_id.address", dia: "$_id.dia", hora: "$_id.hora"},
		rota: {$push: "$rota.res"}
	}},
	{$project: {
		rota: {$reduce: {
			input: {$filter: {input: "$rota", as: "arr", cond: {$not:{$eq:["$$arr", []]}}}}, 
			initialValue: {last: "", res: []}, 
			in: {res: {$concatArrays:[
					"$$value.res",
					{$cond:[
						{$eq: ["$$value.last", "$$this"]},
						[], 
						"$$this"
					]}
				]}, 
				last:"$$this"
			}}
		} 
	}},
	{$group: {
		_id: { address: "$_id.address", dia: "$_id.dia"},
		rota: {$push: "$rota.res"}
	}},
	{$project: {
		rota: {$reduce: {
			input: "$rota", 
			initialValue: {last: "", res: []}, 
			in: {res: {$concatArrays:[
					"$$value.res",
					{$cond:[
						{$eq: ["$$value.last", {$arrayElemAt: ["$$this", 0]}]},
						[{ $slice: [ "$$this", 1, { $size: "$$this" } ] }], 
						"$$this"
					]}
				]}, 
				last:{$arrayElemAt: ["$$this", -1]}
			}}
		} 
	}},
	{$project: {
		rota: {$filter: {input: "$rota.res", as: "arr", cond: {$not:{$eq:["$$arr", []]}}}}
	}}
],{allowDiskUse: true})
