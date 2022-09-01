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

package org.apache.seatunnel.app.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

@Component
public class JwtUtils implements InitializingBean {
    @Value("${jwt.expireTime}")
    private int expireTime;
    @Value("${jwt.secretKey}")
    private String secretKey;
    @Value("${jwt.algorithm}")
    private String algorithmString;
    private SignatureAlgorithm algorithm = null;

    @Override
    public void afterPropertiesSet() throws Exception {
        algorithm = SignatureAlgorithm.valueOf(algorithmString);
    }

    public String genToken(Map<String, Object> data) {
        final Date currentDate = new Date();
        final Date expireDate = DateUtils.addSeconds(currentDate, expireTime);

        return Jwts.builder()
                .signWith(SignatureAlgorithm.HS256, secretKey.getBytes(StandardCharsets.UTF_8))
                .setId(UUID.randomUUID().toString())
                .setClaims(data)
                .setIssuedAt(currentDate)
                .setExpiration(expireDate)
                .compact();
    }

    public Map<String, Object> parseToken(String token) {
        final Jws<Claims> claims = Jwts.parser().setSigningKey(secretKey.getBytes(StandardCharsets.UTF_8)).parseClaimsJws(token);
        return claims.getBody();
    }
}
