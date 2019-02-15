package cn.llj.pers.beans

/**
  * 自定义一个隐式转换的类
  * @param str
  */
class LJStr(str:String) {
  def toIntx:Int={
    try{
      str.toInt
    }catch {
      case _:Exception=>0
    }
  }
  def toDoublex:Double={
    try{
      str.toDouble
    }catch {
      case _:Exception=>0.0
    }
  }
}
object LJStr{
  implicit def str2LJStr(str:String)=new LJStr(str)
}