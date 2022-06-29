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
package org.apache.seatunnel.admin.exception;

import org.apache.seatunnel.admin.enums.ResultStatus;

public class SeatunnelServiceException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private ResultStatus resultStatus;

	public ResultStatus getResultStatus() {
		return resultStatus;
	}

	public SeatunnelServiceException(ResultStatus resultStatus){
		super(resultStatus.getMsg());
		this.resultStatus = resultStatus;
	}

	public SeatunnelServiceException(String message){
		super(message);
		this.resultStatus = ResultStatus.INTERNAL_SERVER_ERROR_ARGS;
	}

	public SeatunnelServiceException(ResultStatus resultStatus, String message){
		super(message);
		this.resultStatus = resultStatus;
	}

	public SeatunnelServiceException(ResultStatus resultStatus, Throwable cause)
	{
		super(cause);
		this.resultStatus = resultStatus;
	}

	public SeatunnelServiceException(ResultStatus resultStatus, String message, Throwable cause)
	{
		super(message,cause);
		this.resultStatus = resultStatus;
	}
}
