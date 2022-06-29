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
package org.apache.seatunnel.admin.service.impl;

import org.apache.seatunnel.admin.entity.StTask;
import org.apache.seatunnel.admin.mapper.StTaskMapper;
import org.apache.seatunnel.admin.service.IStTaskService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.stereotype.Service;

/**
 * <p>
 * 任务表 服务实现类
 * </p>
 *
 * @author quanzhian
 * @since 2022-06-28
 */
@Service
public class StTaskServiceImpl extends ServiceImpl<StTaskMapper, StTask> implements IStTaskService {

}
