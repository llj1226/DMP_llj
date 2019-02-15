package cn.llj.pers.utils

import org.apache.spark.sql.Row

object rptKpiHelper {
  def rptKpi(row: Row) = {
    //提取关键数据项
    val rMode = row.getAs[Int]("requestmode")
    val pNode = row.getAs[Int]("processnode")
    val efftive = row.getAs[Int]("iseffective")
    val bill = row.getAs[Int]("isbilling")
    val bid = row.getAs[Int]("isbid")
    val isWin = row.getAs[Int]("iswin")
    val adOrderId = row.getAs[Int]("adorderid")
    val winp = row.getAs[Double]("winprice")
    val pay = row.getAs[Double]("adpayment")
    //原始请求  有效请求  广告请求
    val adReq = if (rMode == 1 && pNode == 1) {
      List[Double](1, 0, 0)
    } else if (rMode == 1 && pNode == 2) {
      List[Double](1, 1, 0)
    } else if (rMode == 1 && pNode == 3) {
      List[Double](1, 1, 1)
    } else {
      List[Double](0, 0, 0)
    }
    //参与竞价数  竞价成功数   广告成本  广告消费
    val adCost = if (efftive == 1 && bill == 1 && bid == 1 && adOrderId != 0) {
      List[Double](1, 0, 0, 0)
    } else if (efftive == 1 && bill == 1 && isWin == 1) {
      List[Double](0, 1, pay / 1000, winp / 1000)
    } else List[Double](0, 0, 0, 0)

    //展示量  点击量
    val adShow = if (rMode == 2 && efftive == 1) {
      List[Double](1, 0)
    } else if (rMode == 3 && efftive == 1) {
      List[Double](0, 1)
    } else List[Double](0, 0)
    adReq ++ adCost ++ adShow
  }
}
