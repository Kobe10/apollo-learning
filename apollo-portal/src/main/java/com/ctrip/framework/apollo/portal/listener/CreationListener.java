package com.ctrip.framework.apollo.portal.listener;

import com.ctrip.framework.apollo.common.dto.AppDTO;
import com.ctrip.framework.apollo.common.dto.AppNamespaceDTO;
import com.ctrip.framework.apollo.common.utils.BeanUtils;
import com.ctrip.framework.apollo.core.enums.Env;
import com.ctrip.framework.apollo.portal.api.AdminServiceAPI;
import com.ctrip.framework.apollo.portal.component.PortalSettings;
import com.ctrip.framework.apollo.tracer.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 事件创建类
 */
@Component
public class CreationListener {

    private static Logger logger = LoggerFactory.getLogger(CreationListener.class);

    private final PortalSettings portalSettings;
    private final AdminServiceAPI.AppAPI appAPI;
    private final AdminServiceAPI.NamespaceAPI namespaceAPI;

    public CreationListener(
            final PortalSettings portalSettings,
            final AdminServiceAPI.AppAPI appAPI,
            final AdminServiceAPI.NamespaceAPI namespaceAPI) {
        this.portalSettings = portalSettings;
        this.appAPI = appAPI;
        this.namespaceAPI = namespaceAPI;
    }

    /**
     * spring事件监听器 监听AppCreationEvent事件 (监听创建APP的事件   创建app时会发布一个spring事件)
     *
     * @param event
     */
    @EventListener
    public void onAppCreationEvent(AppCreationEvent event) {
        //参数转换
        AppDTO appDTO = BeanUtils.transform(AppDTO.class, event.getApp());
        //遍历环境数组  然后发送http请求 在adminService 和configService中创建 相同的app应用
        List<Env> envs = portalSettings.getActiveEnvs();
        for (Env env : envs) {
            try {
                appAPI.createApp(env, appDTO);
            } catch (Throwable e) {
                logger.error("Create app failed. appId = {}, env = {})", appDTO.getAppId(), env, e);
                Tracer.logError(String.format("Create app failed. appId = %s, env = %s", appDTO.getAppId(), env), e);
            }
        }
    }

    /**
     * spring事件监听器 监听AppNamespaceCreationEvent事件 (监听创建配置文件namespace的事件   创建namespace时会发布一个spring事件)
     *
     * @param event
     */
    @EventListener
    public void onAppNamespaceCreationEvent(AppNamespaceCreationEvent event) {
        AppNamespaceDTO appNamespace = BeanUtils.transform(AppNamespaceDTO.class, event.getAppNamespace());
        List<Env> envs = portalSettings.getActiveEnvs();
        for (Env env : envs) {
            try {
                namespaceAPI.createAppNamespace(env, appNamespace);
            } catch (Throwable e) {
                logger.error("Create appNamespace failed. appId = {}, env = {}", appNamespace.getAppId(), env, e);
                Tracer.logError(String.format("Create appNamespace failed. appId = %s, env = %s", appNamespace.getAppId(), env), e);
            }
        }
    }

}
