/*
Queremos saber cuanto han gastado nuestros cliente en cada año, ordenados de mayor a menor.
*/
db.productos.aggregate([{
        $group: {
            _id: {
                año: {
                    $year: "$fechaVenta"
                },
                cliente: "$cliente"
            },
            dineroGastado: {
                $sum: {
                    $multiply: ["$cantidad", "$precioUnidad"]
                }
            }
        }
    },
    {
        $project: {
            _id: 0,
            cliente: "$_id.cliente",
            año: "$_id.año",
            dineroGastado: {
                $round: ["$dineroGastado"]
            },
        }
    },
    {
        $sort: {
            dineroGastado: -1
        }
    }
])



/*
Queremos saber la cantidad de productos Asus que hemos vendido en cada año
*/
db.productos.aggregate([{
        $match: {
            producto: {
                $regex: /.?asus/i
            }
        }
    }, {
        $group: {
            _id: {
                año: {
                    $year: "$fechaVenta"
                }
            },
            cantidad: {
                $sum: "$cantidad"
            }
        }
    },
    {
        $project: {
            _id: 0,
            Marca: "Asus",
            año: "$_id.año",
            cantidad: "$cantidad"
        }
    }
])



/*
Queremos saber en que meses solemos tener mas cantidad de ventas, por lo que
mostraremos los meses ordenados de mayor a menor segun sus ventas.
*/
db.productos.aggregate([{
        $group: {
            _id: {
                mes: {
                    $month: "$fechaVenta"
                }
            },
            ventas: {
                $sum: "$cantidad"
            }
        }
    },
    {
        $project: {
            _id: 0,
            mes: "$_id.mes",
            ventas: "$ventas",
        }
    },
    {
        $sort: {
            ventas: -1
        }
    }
])


/*
Queremos saber cuales son los articulos mas vendidos por cada año
*/
db.productos.aggregate([{
        $group: {
            _id: {
                año: {
                    $year: "$fechaVenta"
                },
                producto: "$producto"
            },
            ventas: {
                $sum: "$cantidad"
            }
        }
    },
    {
        $project: {
            _id: 0,
            producto: "$_id.producto",
            año: "$_id.año",
            ventas: "$ventas",
        }
    },
    {
        $sort: {
            ventas: -1
        }
    }
])





/*
Queremos saber los beneficios que sacamos en marzo del año 2020
*/
db.productos.aggregate([{
        $match: {
            $and: [{
                    $expr: {
                        $eq: [{
                            $month: "$fechaVenta"
                        }, 3]
                    }
                },
                {
                    $expr: {
                        $eq: [{
                            $year: "$fechaVenta"
                        }, 2020]
                    }
                }
            ]
        }
    }, {
        $project: {
            _id: "$_id",
            venta: {
                $multiply: ["$precioUnidad", "$cantidad"]
            },
            compra: {
                $multiply: ["$costeUnidad", "$cantidad"]
            }
        }
    },
    {
        $project: {
            _id: 0,
            mes: "Marzo",
            año: "2020",
            beneficio: {
                $subtract: [{
                    $sum: "$venta"
                }, {
                    $sum: "$coste"
                }]
            },
        }
    }
])





/*
Queremos saber cuanto dinero le hemos sacado a cada producto de media, independientemente del año de venta
*/
db.productos.aggregate([{
        $group: {
            _id: "$producto",
            venta: {
                $avg: {
                    $multiply: ["$cantidad", "$precioUnidad"]
                }
            },
            coste: {
                $avg: {
                    $multiply: ["$cantidad", "$costeUnidad"]
                }
            }
        }
    },
    {
        $project: {
            _id: 1,
            beneficios: {
                $round: [{
                    $subtract: ["$venta", "$coste"]
                }, 2]
            }
        }
    }
])





/*
Queremos saber cuanto hemos ganado de media por cada categoria en cada año
*/
db.productos.aggregate([{
        $group: {
            _id: {
                año: {
                    $year: "$fechaVenta"
                },
                categoria: "$categoria"
            },
            venta: {
                $avg: {
                    $multiply: ["$cantidad", "$precioUnidad"]
                }
            },
            coste: {
                $avg: {
                    $multiply: ["$cantidad", "$costeUnidad"]
                }
            }
        }
    },
    {
        $project: {
            _id: 0,
            categoria: "$_id.categoria",
            año: "$_id.año",
            beneficios: {
                $round: [{
                    $subtract: ["$venta", "$coste"]
                }, 2]
            }
        }
    },
    {
        $sort: {
            categoria: 1,
            año: 1
        }
    }
])




/*
El cliente Andres Mariscal quiere pagar todos los pedidos a plazos, con un interes del 5%.
Hará el pago en 3 cuotas. Queremos saber cuanto tendra que pagar en cada cuota.
*/
db.productos.aggregate([{
        $match: {
            cliente: {$eq: "Andres Mariscal"}
        }
    },
    {
        $group: {
            _id: "$producto",
            precioTotal: {
                $sum: {
                    $multiply: ["$precioUnidad", "$cantidad"]
                }
            }
        }
    },
    {
        $project: {
            precioPlazo: {
                $divide: [{$multiply: ["$precioTotal",1.05]}, 3]
            }
        }
    }
])