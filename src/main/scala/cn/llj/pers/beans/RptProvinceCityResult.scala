package cn.llj.pers.beans

case class RptProvinceCityResult(pname:String,cname:String,cnt:Int)
//地域报表
case class RptAreaResult(pname:String,
                         cname:String,
                         adRawReq:Double,
                         adEffReq:Double,
                         adReq:Double,
                         adRTBReq:Double,
                         adRTBSuccReq:Double,
                         adCost:Double,
                         adExpense:Double,
                         adShow:Double,
                         adClick:Double
                        )