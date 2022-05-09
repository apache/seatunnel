/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.spark.feishu

object Config extends Serializable {

  final val APP_ID = "appId"
  final val APP_SECRET = "appSecret"
  final val SHEET_TOKEN = "sheetToken"
  final val RANGE = "range"
  final val SHEET_NUM = "sheetNum"

  /**
   * Which line is title line and below this line will save as data
   */
  final val TITLE_LINE_NUM = "titleLineNum"

  /**
   * The title line don't save as data
   */
  final val IGNORE_TITLE_LINE = "ignoreTitleLine"

  // The Feishu response data key
  final val TENANT_ACCESS_TOKEN = "tenant_access_token"
  final val DATA = "data"
  final val VALUE_RANGE = "valueRange"
  final val VALUES = "values"
  final val CODE = "code"
  final val MSG = "msg"
  final val SHEETS = "sheets"
  final val SHEET_ID = "sheetId"

  final val TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal?app_id=%s&app_secret=%s"
  final val META_INFO_URL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/%s/metainfo"
  final val SHEET_DATA_URL = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/%s/values/%s%s?valueRenderOption=ToString&dateTimeRenderOption=FormattedString"
}
