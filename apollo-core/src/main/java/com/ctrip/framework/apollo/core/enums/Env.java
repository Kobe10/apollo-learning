package com.ctrip.framework.apollo.core.enums;

import com.google.common.base.Preconditions;

/**
 * Here is the brief description for all the predefined environments:
 * <ul>
 *   <li>LOCAL: Local Development environment, assume you are working at the beach with no network access</li>
 *   <li>DEV: Development environment</li>
 *   <li>FWS: Feature Web Service Test environment</li>
 *   <li>FAT: Feature Acceptance Test environment</li>
 *   <li>UAT: User Acceptance Test environment</li>
 *   <li>LPT: Load and Performance Test environment</li>
 *   <li>PRO: Production environment</li>
 *   <li>TOOLS: Tooling environment, a special area in production environment which allows
 * access to test environment, e.g. Apollo Portal should be deployed in tools environment</li>
 * </ul>
 *
 * @author Jason Song(song_s@ctrip.com)
 */

/**
 * 环境枚举：前面一直忘记对 Env 介绍，所以有些奇怪的放在这个位置。主要目的是，我们可以参考携程对服务环境的命名和定义。
 */
public enum Env {
    LOCAL, DEV, FWS, FAT, UAT, LPT, PRO, TOOLS, UNKNOWN;

    public static Env fromString(String env) {
        Env environment = EnvUtils.transformEnv(env);
        Preconditions.checkArgument(environment != UNKNOWN, String.format("Env %s is invalid", env));
        return environment;
    }
}
