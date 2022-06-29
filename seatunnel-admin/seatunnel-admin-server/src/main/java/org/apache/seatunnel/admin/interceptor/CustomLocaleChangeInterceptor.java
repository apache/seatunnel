/*
 * Copyright 2002-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.admin.interceptor;

import org.apache.seatunnel.admin.common.Constants;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.util.WebUtils;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Locale;

public class CustomLocaleChangeInterceptor implements HandlerInterceptor {

	@Override
	public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
		Cookie cookie = WebUtils.getCookie(request, Constants.LOCALE_LANGUAGE);
		if (cookie != null) {
			return true;
		}
		String newLocale = request.getHeader(Constants.LOCALE_LANGUAGE);
		if (newLocale != null) {
			LocaleContextHolder.setLocale(parseLocaleValue(newLocale));
			return true;
		}
		newLocale = request.getParameter(Constants.LOCALE_LANGUAGE);
		if (newLocale != null) {
			LocaleContextHolder.setLocale(parseLocaleValue(newLocale));
		}
		return true;
	}

	@Nullable
	protected Locale parseLocaleValue(String localeValue) {
		return StringUtils.parseLocale(localeValue);
	}

}
