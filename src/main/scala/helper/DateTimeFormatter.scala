package helper

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class DateTimeFormatter(datePattern: String) extends Serializable {
  /** return date format to given format
   */
  def formateDate(dateString: String): LocalDateTime = {
    val formatter = DateTimeFormatter.ofPattern(datePattern)
    val dt = LocalDateTime.parse(dateString, formatter)
    dt
  }
}
