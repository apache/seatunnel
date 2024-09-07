import{d as P,h as s,c as d,a as C,u as V,b as ne,e as y,f as ie,g as l,i as b,N as be,j as K,w as me,r as k,t as X,k as He,p as D,l as ke,m as $,n as le,o as _,F as Oe,q as ee,s as Me,v as J,x as ae,y as oe,z as B,A as Y,B as Ee,C as $e,D as I,E as te,G as Le,H as je,I as xe,J as Ke,K as Fe,R as Z,L as Be,M as _e,O as Ve}from"./index-c41f75db.js";import{N as q,g as De,u as Ue}from"./index-53b5e945.js";import{p as ye,l as Ce,C as qe,a as Ge,f as Q,b as ze,N as Ye,c as We,d as Xe,e as he,g as fe}from"./LayoutContent-6ed5d4f6.js";import{u as re,a as Je}from"./service-5d2161a0.js";const Ze=P({name:"ChevronDownFilled",render(){return s("svg",{viewBox:"0 0 16 16",fill:"none",xmlns:"http://www.w3.org/2000/svg"},s("path",{d:"M3.20041 5.73966C3.48226 5.43613 3.95681 5.41856 4.26034 5.70041L8 9.22652L11.7397 5.70041C12.0432 5.41856 12.5177 5.43613 12.7996 5.73966C13.0815 6.0432 13.0639 6.51775 12.7603 6.7996L8.51034 10.7996C8.22258 11.0668 7.77743 11.0668 7.48967 10.7996L3.23966 6.7996C2.93613 6.51775 2.91856 6.0432 3.20041 5.73966Z",fill:"currentColor"}))}}),Qe=d("layout-header",`
 transition:
 color .3s var(--n-bezier),
 background-color .3s var(--n-bezier),
 box-shadow .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
 box-sizing: border-box;
 width: 100%;
 background-color: var(--n-color);
 color: var(--n-text-color);
`,[C("absolute-positioned",`
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 `),C("bordered",`
 border-bottom: solid 1px var(--n-border-color);
 `)]),eo={position:ye,inverted:Boolean,bordered:{type:Boolean,default:!1}},oo=P({name:"LayoutHeader",props:Object.assign(Object.assign({},V.props),eo),setup(e){const{mergedClsPrefixRef:t,inlineThemeDisabled:r}=ne(e),i=V("Layout","-layout-header",Qe,Ce,e,t),c=y(()=>{const{common:{cubicBezierEaseInOut:h},self:v}=i.value,a={"--n-bezier":h};return e.inverted?(a["--n-color"]=v.headerColorInverted,a["--n-text-color"]=v.textColorInverted,a["--n-border-color"]=v.headerBorderColorInverted):(a["--n-color"]=v.headerColor,a["--n-text-color"]=v.textColor,a["--n-border-color"]=v.headerBorderColor),a}),u=r?ie("layout-header",y(()=>e.inverted?"a":"b"),c,e):void 0;return{mergedClsPrefix:t,cssVars:r?void 0:c,themeClass:u==null?void 0:u.themeClass,onRender:u==null?void 0:u.onRender}},render(){var e;const{mergedClsPrefix:t}=this;return(e=this.onRender)===null||e===void 0||e.call(this),s("div",{class:[`${t}-layout-header`,this.themeClass,this.position&&`${t}-layout-header--${this.position}-positioned`,this.bordered&&`${t}-layout-header--bordered`],style:this.cssVars},this.$slots)}}),to=d("layout-sider",`
 flex-shrink: 0;
 box-sizing: border-box;
 position: relative;
 z-index: 1;
 color: var(--n-text-color);
 transition:
 color .3s var(--n-bezier),
 border-color .3s var(--n-bezier),
 min-width .3s var(--n-bezier),
 max-width .3s var(--n-bezier),
 transform .3s var(--n-bezier),
 background-color .3s var(--n-bezier);
 background-color: var(--n-color);
 display: flex;
 justify-content: flex-end;
`,[C("bordered",[l("border",`
 content: "";
 position: absolute;
 top: 0;
 bottom: 0;
 width: 1px;
 background-color: var(--n-border-color);
 transition: background-color .3s var(--n-bezier);
 `)]),l("left-placement",[C("bordered",[l("border",`
 right: 0;
 `)])]),C("right-placement",`
 justify-content: flex-start;
 `,[C("bordered",[l("border",`
 left: 0;
 `)]),C("collapsed",[d("layout-toggle-button",[d("base-icon",`
 transform: rotate(180deg);
 `)]),d("layout-toggle-bar",[b("&:hover",[l("top",{transform:"rotate(-12deg) scale(1.15) translateY(-2px)"}),l("bottom",{transform:"rotate(12deg) scale(1.15) translateY(2px)"})])])]),d("layout-toggle-button",`
 left: 0;
 transform: translateX(-50%) translateY(-50%);
 `,[d("base-icon",`
 transform: rotate(0);
 `)]),d("layout-toggle-bar",`
 left: -28px;
 transform: rotate(180deg);
 `,[b("&:hover",[l("top",{transform:"rotate(12deg) scale(1.15) translateY(-2px)"}),l("bottom",{transform:"rotate(-12deg) scale(1.15) translateY(2px)"})])])]),C("collapsed",[d("layout-toggle-bar",[b("&:hover",[l("top",{transform:"rotate(-12deg) scale(1.15) translateY(-2px)"}),l("bottom",{transform:"rotate(12deg) scale(1.15) translateY(2px)"})])]),d("layout-toggle-button",[d("base-icon",`
 transform: rotate(0);
 `)])]),d("layout-toggle-button",`
 transition:
 color .3s var(--n-bezier),
 right .3s var(--n-bezier),
 left .3s var(--n-bezier),
 border-color .3s var(--n-bezier),
 background-color .3s var(--n-bezier);
 cursor: pointer;
 width: 24px;
 height: 24px;
 position: absolute;
 top: 50%;
 right: 0;
 border-radius: 50%;
 display: flex;
 align-items: center;
 justify-content: center;
 font-size: 18px;
 color: var(--n-toggle-button-icon-color);
 border: var(--n-toggle-button-border);
 background-color: var(--n-toggle-button-color);
 box-shadow: 0 2px 4px 0px rgba(0, 0, 0, .06);
 transform: translateX(50%) translateY(-50%);
 z-index: 1;
 `,[d("base-icon",`
 transition: transform .3s var(--n-bezier);
 transform: rotate(180deg);
 `)]),d("layout-toggle-bar",`
 cursor: pointer;
 height: 72px;
 width: 32px;
 position: absolute;
 top: calc(50% - 36px);
 right: -28px;
 `,[l("top, bottom",`
 position: absolute;
 width: 4px;
 border-radius: 2px;
 height: 38px;
 left: 14px;
 transition: 
 background-color .3s var(--n-bezier),
 transform .3s var(--n-bezier);
 `),l("bottom",`
 position: absolute;
 top: 34px;
 `),b("&:hover",[l("top",{transform:"rotate(12deg) scale(1.15) translateY(-2px)"}),l("bottom",{transform:"rotate(-12deg) scale(1.15) translateY(2px)"})]),l("top, bottom",{backgroundColor:"var(--n-toggle-bar-color)"}),b("&:hover",[l("top, bottom",{backgroundColor:"var(--n-toggle-bar-color-hover)"})])]),l("border",`
 position: absolute;
 top: 0;
 right: 0;
 bottom: 0;
 width: 1px;
 transition: background-color .3s var(--n-bezier);
 `),d("layout-sider-scroll-container",`
 flex-grow: 1;
 flex-shrink: 0;
 box-sizing: border-box;
 height: 100%;
 opacity: 0;
 transition: opacity .3s var(--n-bezier);
 max-width: 100%;
 `),C("show-content",[d("layout-sider-scroll-container",{opacity:1})]),C("absolute-positioned",`
 position: absolute;
 left: 0;
 top: 0;
 bottom: 0;
 `)]),ro=P({name:"LayoutToggleButton",props:{clsPrefix:{type:String,required:!0},onClick:Function},render(){const{clsPrefix:e}=this;return s("div",{class:`${e}-layout-toggle-button`,onClick:this.onClick},s(be,{clsPrefix:e},{default:()=>s(qe,null)}))}}),no=P({props:{clsPrefix:{type:String,required:!0},onClick:Function},render(){const{clsPrefix:e}=this;return s("div",{onClick:this.onClick,class:`${e}-layout-toggle-bar`},s("div",{class:`${e}-layout-toggle-bar__top`}),s("div",{class:`${e}-layout-toggle-bar__bottom`}))}}),io={position:ye,bordered:Boolean,collapsedWidth:{type:Number,default:48},width:{type:[Number,String],default:272},contentStyle:{type:[String,Object],default:""},collapseMode:{type:String,default:"transform"},collapsed:{type:Boolean,default:void 0},defaultCollapsed:Boolean,showCollapsedContent:{type:Boolean,default:!0},showTrigger:{type:[Boolean,String],default:!1},nativeScrollbar:{type:Boolean,default:!0},inverted:Boolean,scrollbarProps:Object,triggerStyle:[String,Object],collapsedTriggerStyle:[String,Object],"onUpdate:collapsed":[Function,Array],onUpdateCollapsed:[Function,Array],onAfterEnter:Function,onAfterLeave:Function,onExpand:[Function,Array],onCollapse:[Function,Array],onScroll:Function},lo=P({name:"LayoutSider",props:Object.assign(Object.assign({},V.props),io),setup(e){const t=K(Ge);t?t.hasSider||me("layout-sider","You are putting `n-layout-sider` in a `n-layout` but haven't set `has-sider` on the `n-layout`."):me("layout-sider","Layout sider is not allowed to be put outside layout.");const r=k(null),i=k(null),c=y(()=>Q(a.value?e.collapsedWidth:e.width)),u=y(()=>e.collapseMode!=="transform"?{}:{minWidth:Q(e.width)}),h=y(()=>t?t.siderPlacement:"left"),v=k(e.defaultCollapsed),a=re(X(e,"collapsed"),v);function N(w,x){if(e.nativeScrollbar){const{value:z}=r;z&&(x===void 0?z.scrollTo(w):z.scrollTo(w,x))}else{const{value:z}=i;z&&z.scrollTo(w,x)}}function M(){const{"onUpdate:collapsed":w,onUpdateCollapsed:x,onExpand:z,onCollapse:f}=e,{value:g}=a;x&&$(x,!g),w&&$(w,!g),v.value=!g,g?z&&$(z):f&&$(f)}let S=0,p=0;const T=w=>{var x;const z=w.target;S=z.scrollLeft,p=z.scrollTop,(x=e.onScroll)===null||x===void 0||x.call(e,w)};He(()=>{if(e.nativeScrollbar){const w=r.value;w&&(w.scrollTop=p,w.scrollLeft=S)}}),D(ze,{collapsedRef:a,collapseModeRef:X(e,"collapseMode")});const{mergedClsPrefixRef:H,inlineThemeDisabled:A}=ne(e),O=V("Layout","-layout-sider",to,Ce,e,H);function L(w){var x,z;w.propertyName==="max-width"&&(a.value?(x=e.onAfterLeave)===null||x===void 0||x.call(e):(z=e.onAfterEnter)===null||z===void 0||z.call(e))}const U={scrollTo:N},j=y(()=>{const{common:{cubicBezierEaseInOut:w},self:x}=O.value,{siderToggleButtonColor:z,siderToggleButtonBorder:f,siderToggleBarColor:g,siderToggleBarColorHover:o}=x,m={"--n-bezier":w,"--n-toggle-button-color":z,"--n-toggle-button-border":f,"--n-toggle-bar-color":g,"--n-toggle-bar-color-hover":o};return e.inverted?(m["--n-color"]=x.siderColorInverted,m["--n-text-color"]=x.textColorInverted,m["--n-border-color"]=x.siderBorderColorInverted,m["--n-toggle-button-icon-color"]=x.siderToggleButtonIconColorInverted,m.__invertScrollbar=x.__invertScrollbar):(m["--n-color"]=x.siderColor,m["--n-text-color"]=x.textColor,m["--n-border-color"]=x.siderBorderColor,m["--n-toggle-button-icon-color"]=x.siderToggleButtonIconColor),m}),E=A?ie("layout-sider",y(()=>e.inverted?"a":"b"),j,e):void 0;return Object.assign({scrollableElRef:r,scrollbarInstRef:i,mergedClsPrefix:H,mergedTheme:O,styleMaxWidth:c,mergedCollapsed:a,scrollContainerStyle:u,siderPlacement:h,handleNativeElScroll:T,handleTransitionend:L,handleTriggerClick:M,inlineThemeDisabled:A,cssVars:j,themeClass:E==null?void 0:E.themeClass,onRender:E==null?void 0:E.onRender},U)},render(){var e;const{mergedClsPrefix:t,mergedCollapsed:r,showTrigger:i}=this;return(e=this.onRender)===null||e===void 0||e.call(this),s("aside",{class:[`${t}-layout-sider`,this.themeClass,`${t}-layout-sider--${this.position}-positioned`,`${t}-layout-sider--${this.siderPlacement}-placement`,this.bordered&&`${t}-layout-sider--bordered`,r&&`${t}-layout-sider--collapsed`,(!r||this.showCollapsedContent)&&`${t}-layout-sider--show-content`],onTransitionend:this.handleTransitionend,style:[this.inlineThemeDisabled?void 0:this.cssVars,{maxWidth:this.styleMaxWidth,width:Q(this.width)}]},this.nativeScrollbar?s("div",{class:`${t}-layout-sider-scroll-container`,onScroll:this.handleNativeElScroll,style:[this.scrollContainerStyle,{overflow:"auto"},this.contentStyle],ref:"scrollableElRef"},this.$slots):s(ke,Object.assign({},this.scrollbarProps,{onScroll:this.onScroll,ref:"scrollbarInstRef",style:this.scrollContainerStyle,contentStyle:this.contentStyle,theme:this.mergedTheme.peers.Scrollbar,themeOverrides:this.mergedTheme.peerOverrides.Scrollbar,builtinThemeOverrides:this.inverted&&this.cssVars.__invertScrollbar==="true"?{colorHover:"rgba(255, 255, 255, .4)",color:"rgba(255, 255, 255, .3)"}:void 0}),this.$slots),i?i==="bar"?s(no,{clsPrefix:t,style:r?this.collapsedTriggerStyle:this.triggerStyle,onClick:this.handleTriggerClick}):s(ro,{clsPrefix:t,style:r?this.collapsedTriggerStyle:this.triggerStyle,onClick:this.handleTriggerClick}):null,this.bordered?s("div",{class:`${t}-layout-sider__border`}):null)}}),G=le("n-menu"),ce=le("n-submenu"),de=le("n-menu-item-group"),W=8;function se(e){const t=K(G),{props:r,mergedCollapsedRef:i}=t,c=K(ce,null),u=K(de,null),h=y(()=>r.mode==="horizontal"),v=y(()=>h.value?r.dropdownPlacement:"tmNodes"in e?"right-start":"right"),a=y(()=>{var p;return Math.max((p=r.collapsedIconSize)!==null&&p!==void 0?p:r.iconSize,r.iconSize)}),N=y(()=>{var p;return!h.value&&e.root&&i.value&&(p=r.collapsedIconSize)!==null&&p!==void 0?p:r.iconSize}),M=y(()=>{if(h.value)return;const{collapsedWidth:p,indent:T,rootIndent:H}=r,{root:A,isGroup:O}=e,L=H===void 0?T:H;if(A)return i.value?p/2-a.value/2:L;if(u)return T/2+u.paddingLeftRef.value;if(c)return(O?T/2:T)+c.paddingLeftRef.value}),S=y(()=>{const{collapsedWidth:p,indent:T,rootIndent:H}=r,{value:A}=a,{root:O}=e;return h.value||!O||!i.value?W:(H===void 0?T:H)+A+W-(p+A)/2});return{dropdownPlacement:v,activeIconSize:N,maxIconSize:a,paddingLeft:M,iconMarginRight:S,NMenu:t,NSubmenu:c}}const ue={internalKey:{type:[String,Number],required:!0},root:Boolean,isGroup:Boolean,level:{type:Number,required:!0},title:[String,Function],extra:[String,Function]},we=Object.assign(Object.assign({},ue),{tmNode:{type:Object,required:!0},tmNodes:{type:Array,required:!0}}),ao=P({name:"MenuOptionGroup",props:we,setup(e){D(ce,null);const t=se(e);D(de,{paddingLeftRef:t.paddingLeft});const{mergedClsPrefixRef:r,props:i}=K(G);return function(){const{value:c}=r,u=t.paddingLeft.value,{nodeProps:h}=i,v=h==null?void 0:h(e.tmNode.rawNode);return s("div",{class:`${c}-menu-item-group`,role:"group"},s("div",Object.assign({},v,{class:[`${c}-menu-item-group-title`,v==null?void 0:v.class],style:[(v==null?void 0:v.style)||"",u!==void 0?`padding-left: ${u}px;`:""]}),_(e.title),e.extra?s(Oe,null," ",_(e.extra)):null),s("div",null,e.tmNodes.map(a=>ve(a,i))))}}}),Ie=P({name:"MenuOptionContent",props:{collapsed:Boolean,disabled:Boolean,title:[String,Function],icon:Function,extra:[String,Function],showArrow:Boolean,childActive:Boolean,hover:Boolean,paddingLeft:Number,selected:Boolean,maxIconSize:{type:Number,required:!0},activeIconSize:{type:Number,required:!0},iconMarginRight:{type:Number,required:!0},clsPrefix:{type:String,required:!0},onClick:Function,tmNode:{type:Object,required:!0}},setup(e){const{props:t}=K(G);return{menuProps:t,style:y(()=>{const{paddingLeft:r}=e;return{paddingLeft:r&&`${r}px`}}),iconStyle:y(()=>{const{maxIconSize:r,activeIconSize:i,iconMarginRight:c}=e;return{width:`${r}px`,height:`${r}px`,fontSize:`${i}px`,marginRight:`${c}px`}})}},render(){const{clsPrefix:e,tmNode:t,menuProps:{renderIcon:r,renderLabel:i,renderExtra:c,expandIcon:u}}=this,h=r?r(t.rawNode):_(this.icon);return s("div",{onClick:v=>{var a;(a=this.onClick)===null||a===void 0||a.call(this,v)},role:"none",class:[`${e}-menu-item-content`,{[`${e}-menu-item-content--selected`]:this.selected,[`${e}-menu-item-content--collapsed`]:this.collapsed,[`${e}-menu-item-content--child-active`]:this.childActive,[`${e}-menu-item-content--disabled`]:this.disabled,[`${e}-menu-item-content--hover`]:this.hover}],style:this.style},h&&s("div",{class:`${e}-menu-item-content__icon`,style:this.iconStyle,role:"none"},[h]),s("div",{class:`${e}-menu-item-content-header`,role:"none"},i?i(t.rawNode):_(this.title),this.extra||c?s("span",{class:`${e}-menu-item-content-header__extra`}," ",c?c(t.rawNode):_(this.extra)):null),this.showArrow?s(be,{ariaHidden:!0,class:`${e}-menu-item-content__arrow`,clsPrefix:e},{default:()=>u?u(t.rawNode):s(Ze,null)}):null)}}),Se=Object.assign(Object.assign({},ue),{rawNodes:{type:Array,default:()=>[]},tmNodes:{type:Array,default:()=>[]},tmNode:{type:Object,required:!0},disabled:{type:Boolean,default:!1},icon:Function,onClick:Function}),co=P({name:"Submenu",props:Se,setup(e){const t=se(e),{NMenu:r,NSubmenu:i}=t,{props:c,mergedCollapsedRef:u,mergedThemeRef:h}=r,v=y(()=>{const{disabled:p}=e;return i!=null&&i.mergedDisabledRef.value||c.disabled?!0:p}),a=k(!1);D(ce,{paddingLeftRef:t.paddingLeft,mergedDisabledRef:v}),D(de,null);function N(){const{onClick:p}=e;p&&p()}function M(){v.value||(u.value||r.toggleExpand(e.internalKey),N())}function S(p){a.value=p}return{menuProps:c,mergedTheme:h,doSelect:r.doSelect,inverted:r.invertedRef,isHorizontal:r.isHorizontalRef,mergedClsPrefix:r.mergedClsPrefixRef,maxIconSize:t.maxIconSize,activeIconSize:t.activeIconSize,iconMarginRight:t.iconMarginRight,dropdownPlacement:t.dropdownPlacement,dropdownShow:a,paddingLeft:t.paddingLeft,mergedDisabled:v,mergedValue:r.mergedValueRef,childActive:ee(()=>r.activePathRef.value.includes(e.internalKey)),collapsed:y(()=>c.mode==="horizontal"?!1:u.value?!0:!r.mergedExpandedKeysRef.value.includes(e.internalKey)),dropdownEnabled:y(()=>!v.value&&(c.mode==="horizontal"||u.value)),handlePopoverShowChange:S,handleClick:M}},render(){var e;const{mergedClsPrefix:t,menuProps:{renderIcon:r,renderLabel:i}}=this,c=()=>{const{isHorizontal:h,paddingLeft:v,collapsed:a,mergedDisabled:N,maxIconSize:M,activeIconSize:S,title:p,childActive:T,icon:H,handleClick:A,menuProps:{nodeProps:O},dropdownShow:L,iconMarginRight:U,tmNode:j,mergedClsPrefix:E}=this,w=O==null?void 0:O(j.rawNode);return s("div",Object.assign({},w,{class:[`${E}-menu-item`,w==null?void 0:w.class],role:"menuitem"}),s(Ie,{tmNode:j,paddingLeft:v,collapsed:a,disabled:N,iconMarginRight:U,maxIconSize:M,activeIconSize:S,title:p,extra:this.extra,showArrow:!h,childActive:T,clsPrefix:E,icon:H,hover:L,onClick:A}))},u=()=>s(Me,null,{default:()=>{const{tmNodes:h,collapsed:v}=this;return v?null:s("div",{class:`${t}-submenu-children`,role:"menu"},h.map(a=>ve(a,this.menuProps)))}});return this.root?s(Ye,Object.assign({size:"large",trigger:"hover"},(e=this.menuProps)===null||e===void 0?void 0:e.dropdownProps,{themeOverrides:this.mergedTheme.peerOverrides.Dropdown,theme:this.mergedTheme.peers.Dropdown,builtinThemeOverrides:{fontSizeLarge:"14px",optionIconSizeLarge:"18px"},value:this.mergedValue,disabled:!this.dropdownEnabled,placement:this.dropdownPlacement,keyField:this.menuProps.keyField,labelField:this.menuProps.labelField,childrenField:this.menuProps.childrenField,onUpdateShow:this.handlePopoverShowChange,options:this.rawNodes,onSelect:this.doSelect,inverted:this.inverted,renderIcon:r,renderLabel:i}),{default:()=>s("div",{class:`${t}-submenu`,role:"menuitem","aria-expanded":!this.collapsed},c(),this.isHorizontal?null:u())}):s("div",{class:`${t}-submenu`,role:"menuitem","aria-expanded":!this.collapsed},c(),u())}}),Re=Object.assign(Object.assign({},ue),{tmNode:{type:Object,required:!0},disabled:Boolean,icon:Function,onClick:Function}),so=P({name:"MenuOption",props:Re,setup(e){const t=se(e),{NSubmenu:r,NMenu:i}=t,{props:c,mergedClsPrefixRef:u,mergedCollapsedRef:h}=i,v=r?r.mergedDisabledRef:{value:!1},a=y(()=>v.value||e.disabled);function N(S){const{onClick:p}=e;p&&p(S)}function M(S){a.value||(i.doSelect(e.internalKey,e.tmNode.rawNode),N(S))}return{mergedClsPrefix:u,dropdownPlacement:t.dropdownPlacement,paddingLeft:t.paddingLeft,iconMarginRight:t.iconMarginRight,maxIconSize:t.maxIconSize,activeIconSize:t.activeIconSize,mergedTheme:i.mergedThemeRef,menuProps:c,dropdownEnabled:ee(()=>e.root&&h.value&&c.mode!=="horizontal"&&!a.value),selected:ee(()=>i.mergedValueRef.value===e.internalKey),mergedDisabled:a,handleClick:M}},render(){const{mergedClsPrefix:e,mergedTheme:t,tmNode:r,menuProps:{renderLabel:i,nodeProps:c}}=this,u=c==null?void 0:c(r.rawNode);return s("div",Object.assign({},u,{role:"menuitem",class:[`${e}-menu-item`,u==null?void 0:u.class]}),s(We,{theme:t.peers.Tooltip,themeOverrides:t.peerOverrides.Tooltip,trigger:"hover",placement:this.dropdownPlacement,disabled:!this.dropdownEnabled||this.title===void 0,internalExtraClass:["menu-tooltip"]},{default:()=>i?i(r.rawNode):_(this.title),trigger:()=>s(Ie,{tmNode:r,clsPrefix:e,paddingLeft:this.paddingLeft,iconMarginRight:this.iconMarginRight,maxIconSize:this.maxIconSize,activeIconSize:this.activeIconSize,selected:this.selected,title:this.title,extra:this.extra,disabled:this.mergedDisabled,icon:this.icon,onClick:this.handleClick})}))}}),uo=P({name:"MenuDivider",setup(){const e=K(G),{mergedClsPrefixRef:t,isHorizontalRef:r}=e;return()=>r.value?null:s("div",{class:`${t.value}-menu-divider`})}}),vo=ae(we),mo=ae(Re),ho=ae(Se);function Pe(e){return e.type==="divider"||e.type==="render"}function fo(e){return e.type==="divider"}function ve(e,t){const{rawNode:r}=e,{show:i}=r;if(i===!1)return null;if(Pe(r))return fo(r)?s(uo,Object.assign({key:e.key},r.props)):null;const{labelField:c}=t,{key:u,level:h,isGroup:v}=e,a=Object.assign(Object.assign({},r),{title:r.title||r[c],extra:r.titleExtra||r.extra,key:u,internalKey:u,level:h,root:h===0,isGroup:v});return e.children?e.isGroup?s(ao,J(a,vo,{tmNode:e,tmNodes:e.children,key:u})):s(co,J(a,ho,{key:u,rawNodes:r[t.childrenField],tmNodes:e.children,tmNode:e})):s(so,J(a,mo,{key:u,tmNode:e}))}function po(e){oe(()=>{e.items&&B("menu","`items` is deprecated, please use `options` instead."),e.onOpenNamesChange&&B("menu","`on-open-names-change` is deprecated, please use `on-update:expanded-keys` instead."),e.onSelect&&B("menu","`on-select` is deprecated, please use `on-update:value` instead."),e.onExpandedNamesChange&&B("menu","`on-expanded-names-change` is deprecated, please use `on-update:expanded-keys` instead."),e.expandedNames&&B("menu","`expanded-names` is deprecated, please use `expanded-keys` instead."),e.defaultExpandedNames&&B("menu","`default-expanded-names` is deprecated, please use `default-expanded-keys` instead.")})}const pe=[b("&::before","background-color: var(--n-item-color-hover);"),l("arrow",`
 color: var(--n-arrow-color-hover);
 `),l("icon",`
 color: var(--n-item-icon-color-hover);
 `),d("menu-item-content-header",`
 color: var(--n-item-text-color-hover);
 `,[b("a",`
 color: var(--n-item-text-color-hover);
 `),l("extra",`
 color: var(--n-item-text-color-hover);
 `)])],ge=[l("icon",`
 color: var(--n-item-icon-color-hover-horizontal);
 `),d("menu-item-content-header",`
 color: var(--n-item-text-color-hover-horizontal);
 `,[b("a",`
 color: var(--n-item-text-color-hover-horizontal);
 `),l("extra",`
 color: var(--n-item-text-color-hover-horizontal);
 `)])],go=b([d("menu",`
 background-color: var(--n-color);
 color: var(--n-item-text-color);
 overflow: hidden;
 transition: background-color .3s var(--n-bezier);
 box-sizing: border-box;
 font-size: var(--n-font-size);
 padding-bottom: 6px;
 `,[C("horizontal",`
 display: inline-flex;
 padding-bottom: 0;
 `,[d("submenu","margin: 0;"),d("menu-item","margin: 0;"),d("menu-item-content",`
 padding: 0 20px;
 border-bottom: 2px solid #0000;
 `,[b("&::before","display: none;"),C("selected","border-bottom: 2px solid var(--n-border-color-horizontal)")]),d("menu-item-content",[C("selected",[l("icon","color: var(--n-item-icon-color-active-horizontal);"),d("menu-item-content-header",`
 color: var(--n-item-text-color-active-horizontal);
 `,[b("a","color: var(--n-item-text-color-active-horizontal);"),l("extra","color: var(--n-item-text-color-active-horizontal);")])]),C("child-active",`
 border-bottom: 2px solid var(--n-border-color-horizontal);
 `,[d("menu-item-content-header",`
 color: var(--n-item-text-color-child-active-horizontal);
 `,[b("a",`
 color: var(--n-item-text-color-child-active-horizontal);
 `),l("extra",`
 color: var(--n-item-text-color-child-active-horizontal);
 `)]),l("icon",`
 color: var(--n-item-icon-color-child-active-horizontal);
 `)]),Y("disabled",[Y("selected, child-active",[b("&:focus-within",ge)]),C("selected",[F(null,[l("icon","color: var(--n-item-icon-color-active-hover-horizontal);"),d("menu-item-content-header",`
 color: var(--n-item-text-color-active-hover-horizontal);
 `,[b("a","color: var(--n-item-text-color-active-hover-horizontal);"),l("extra","color: var(--n-item-text-color-active-hover-horizontal);")])])]),C("child-active",[F(null,[l("icon","color: var(--n-item-icon-color-child-active-hover-horizontal);"),d("menu-item-content-header",`
 color: var(--n-item-text-color-child-active-hover-horizontal);
 `,[b("a","color: var(--n-item-text-color-child-active-hover-horizontal);"),l("extra","color: var(--n-item-text-color-child-active-hover-horizontal);")])])]),F("border-bottom: 2px solid var(--n-border-color-horizontal);",ge)]),d("menu-item-content-header",[b("a","color: var(--n-item-text-color-horizontal);")])])]),C("collapsed",[d("menu-item-content",[C("selected",[b("&::before",`
 background-color: var(--n-item-color-active-collapsed) !important;
 `)]),d("menu-item-content-header","opacity: 0;"),l("arrow","opacity: 0;"),l("icon","color: var(--n-item-icon-color-collapsed);")])]),d("menu-item",`
 height: var(--n-item-height);
 margin-top: 6px;
 position: relative;
 `),d("menu-item-content",`
 box-sizing: border-box;
 line-height: 1.75;
 height: 100%;
 display: grid;
 grid-template-areas: "icon content arrow";
 grid-template-columns: auto 1fr auto;
 align-items: center;
 cursor: pointer;
 position: relative;
 padding-right: 18px;
 transition:
 background-color .3s var(--n-bezier),
 padding-left .3s var(--n-bezier),
 border-color .3s var(--n-bezier);
 `,[b("> *","z-index: 1;"),b("&::before",`
 z-index: auto;
 content: "";
 background-color: #0000;
 position: absolute;
 left: 8px;
 right: 8px;
 top: 0;
 bottom: 0;
 pointer-events: none;
 border-radius: var(--n-border-radius);
 transition: background-color .3s var(--n-bezier);
 `),C("disabled",`
 opacity: .45;
 cursor: not-allowed;
 `),C("collapsed",[l("arrow","transform: rotate(0);")]),C("selected",[b("&::before","background-color: var(--n-item-color-active);"),l("arrow","color: var(--n-arrow-color-active);"),l("icon","color: var(--n-item-icon-color-active);"),d("menu-item-content-header",`
 color: var(--n-item-text-color-active);
 `,[b("a","color: var(--n-item-text-color-active);"),l("extra","color: var(--n-item-text-color-active);")])]),C("child-active",[d("menu-item-content-header",`
 color: var(--n-item-text-color-child-active);
 `,[b("a",`
 color: var(--n-item-text-color-child-active);
 `),l("extra",`
 color: var(--n-item-text-color-child-active);
 `)]),l("arrow",`
 color: var(--n-arrow-color-child-active);
 `),l("icon",`
 color: var(--n-item-icon-color-child-active);
 `)]),Y("disabled",[Y("selected, child-active",[b("&:focus-within",pe)]),C("selected",[F(null,[l("arrow","color: var(--n-arrow-color-active-hover);"),l("icon","color: var(--n-item-icon-color-active-hover);"),d("menu-item-content-header",`
 color: var(--n-item-text-color-active-hover);
 `,[b("a","color: var(--n-item-text-color-active-hover);"),l("extra","color: var(--n-item-text-color-active-hover);")])])]),C("child-active",[F(null,[l("arrow","color: var(--n-arrow-color-child-active-hover);"),l("icon","color: var(--n-item-icon-color-child-active-hover);"),d("menu-item-content-header",`
 color: var(--n-item-text-color-child-active-hover);
 `,[b("a","color: var(--n-item-text-color-child-active-hover);"),l("extra","color: var(--n-item-text-color-child-active-hover);")])])]),C("selected",[F(null,[b("&::before","background-color: var(--n-item-color-active-hover);")])]),F(null,pe)]),l("icon",`
 grid-area: icon;
 color: var(--n-item-icon-color);
 transition:
 color .3s var(--n-bezier),
 font-size .3s var(--n-bezier),
 margin-right .3s var(--n-bezier);
 box-sizing: content-box;
 display: inline-flex;
 align-items: center;
 justify-content: center;
 `),l("arrow",`
 grid-area: arrow;
 font-size: 16px;
 color: var(--n-arrow-color);
 transform: rotate(180deg);
 opacity: 1;
 transition:
 color .3s var(--n-bezier),
 transform 0.2s var(--n-bezier),
 opacity 0.2s var(--n-bezier);
 `),d("menu-item-content-header",`
 grid-area: content;
 transition:
 color .3s var(--n-bezier),
 opacity .3s var(--n-bezier);
 opacity: 1;
 white-space: nowrap;
 overflow: hidden;
 text-overflow: ellipsis;
 color: var(--n-item-text-color);
 `,[b("a",`
 outline: none;
 text-decoration: none;
 transition: color .3s var(--n-bezier);
 color: var(--n-item-text-color);
 `,[b("&::before",`
 content: "";
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 `)]),l("extra",`
 font-size: .93em;
 color: var(--n-group-text-color);
 transition: color .3s var(--n-bezier);
 `)])]),d("submenu",`
 cursor: pointer;
 position: relative;
 margin-top: 6px;
 `,[d("menu-item-content",`
 height: var(--n-item-height);
 `),d("submenu-children",`
 overflow: hidden;
 padding: 0;
 `,[Ee({duration:".2s"})])]),d("menu-item-group",[d("menu-item-group-title",`
 margin-top: 6px;
 color: var(--n-group-text-color);
 cursor: default;
 font-size: .93em;
 height: 36px;
 display: flex;
 align-items: center;
 transition:
 padding-left .3s var(--n-bezier),
 color .3s var(--n-bezier);
 `)])]),d("menu-tooltip",[b("a",`
 color: inherit;
 text-decoration: none;
 `)]),d("menu-divider",`
 transition: background-color .3s var(--n-bezier);
 background-color: var(--n-divider-color);
 height: 1px;
 margin: 6px 18px;
 `)]);function F(e,t){return[C("hover",e,t),b("&:hover",e,t)]}const bo=Object.assign(Object.assign({},V.props),{options:{type:Array,default:()=>[]},collapsed:{type:Boolean,default:void 0},collapsedWidth:{type:Number,default:48},iconSize:{type:Number,default:20},collapsedIconSize:{type:Number,default:24},rootIndent:Number,indent:{type:Number,default:32},labelField:{type:String,default:"label"},keyField:{type:String,default:"key"},childrenField:{type:String,default:"children"},disabledField:{type:String,default:"disabled"},defaultExpandAll:Boolean,defaultExpandedKeys:Array,expandedKeys:Array,value:[String,Number],defaultValue:{type:[String,Number],default:null},mode:{type:String,default:"vertical"},watchProps:{type:Array,default:void 0},disabled:Boolean,show:{type:Boolean,defalut:!0},inverted:Boolean,"onUpdate:expandedKeys":[Function,Array],onUpdateExpandedKeys:[Function,Array],onUpdateValue:[Function,Array],"onUpdate:value":[Function,Array],expandIcon:Function,renderIcon:Function,renderLabel:Function,renderExtra:Function,dropdownProps:Object,accordion:Boolean,nodeProps:Function,items:Array,onOpenNamesChange:[Function,Array],onSelect:[Function,Array],onExpandedNamesChange:[Function,Array],expandedNames:Array,defaultExpandedNames:Array,dropdownPlacement:{type:String,default:"bottom"}}),xo=P({name:"Menu",props:bo,setup(e){po(e);const{mergedClsPrefixRef:t,inlineThemeDisabled:r}=ne(e),i=V("Menu","-menu",go,$e,e,t),c=K(ze,null),u=y(()=>{var f;const{collapsed:g}=e;if(g!==void 0)return g;if(c){const{collapseModeRef:o,collapsedRef:m}=c;if(o.value==="width")return(f=m.value)!==null&&f!==void 0?f:!1}return!1}),h=y(()=>{const{keyField:f,childrenField:g,disabledField:o}=e;return Xe(e.items||e.options,{getIgnored(m){return Pe(m)},getChildren(m){return m[g]},getDisabled(m){return m[o]},getKey(m){var R;return(R=m[f])!==null&&R!==void 0?R:m.name}})}),v=y(()=>new Set(h.value.treeNodes.map(f=>f.key))),{watchProps:a}=e,N=k(null);a!=null&&a.includes("defaultValue")?oe(()=>{N.value=e.defaultValue}):N.value=e.defaultValue;const M=X(e,"value"),S=re(M,N),p=k([]),T=()=>{p.value=e.defaultExpandAll?h.value.getNonLeafKeys():e.defaultExpandedNames||e.defaultExpandedKeys||h.value.getPath(S.value,{includeSelf:!1}).keyPath};a!=null&&a.includes("defaultExpandedKeys")?oe(T):T();const H=Je(e,["expandedNames","expandedKeys"]),A=re(H,p),O=y(()=>h.value.treeNodes),L=y(()=>h.value.getPath(S.value).keyPath);D(G,{props:e,mergedCollapsedRef:u,mergedThemeRef:i,mergedValueRef:S,mergedExpandedKeysRef:A,activePathRef:L,mergedClsPrefixRef:t,isHorizontalRef:y(()=>e.mode==="horizontal"),invertedRef:X(e,"inverted"),doSelect:U,toggleExpand:E});function U(f,g){const{"onUpdate:value":o,onUpdateValue:m,onSelect:R}=e;m&&$(m,f,g),o&&$(o,f,g),R&&$(R,f,g),N.value=f}function j(f){const{"onUpdate:expandedKeys":g,onUpdateExpandedKeys:o,onExpandedNamesChange:m,onOpenNamesChange:R}=e;g&&$(g,f),o&&$(o,f),m&&$(m,f),R&&$(R,f),p.value=f}function E(f){const g=Array.from(A.value),o=g.findIndex(m=>m===f);if(~o)g.splice(o,1);else{if(e.accordion&&v.value.has(f)){const m=g.findIndex(R=>v.value.has(R));m>-1&&g.splice(m,1)}g.push(f)}j(g)}const w=f=>{const g=h.value.getPath(f??S.value,{includeSelf:!1}).keyPath;if(!g.length)return;const o=Array.from(A.value),m=new Set([...o,...g]);e.accordion&&v.value.forEach(R=>{m.has(R)&&!g.includes(R)&&m.delete(R)}),j(Array.from(m))},x=y(()=>{const{inverted:f}=e,{common:{cubicBezierEaseInOut:g},self:o}=i.value,{borderRadius:m,borderColorHorizontal:R,fontSize:Ne,itemHeight:Ae,dividerColor:Te}=o,n={"--n-divider-color":Te,"--n-bezier":g,"--n-font-size":Ne,"--n-border-color-horizontal":R,"--n-border-radius":m,"--n-item-height":Ae};return f?(n["--n-group-text-color"]=o.groupTextColorInverted,n["--n-color"]=o.colorInverted,n["--n-item-text-color"]=o.itemTextColorInverted,n["--n-item-text-color-hover"]=o.itemTextColorHoverInverted,n["--n-item-text-color-active"]=o.itemTextColorActiveInverted,n["--n-item-text-color-child-active"]=o.itemTextColorChildActiveInverted,n["--n-item-text-color-child-active-hover"]=o.itemTextColorChildActiveInverted,n["--n-item-text-color-active-hover"]=o.itemTextColorActiveHoverInverted,n["--n-item-icon-color"]=o.itemIconColorInverted,n["--n-item-icon-color-hover"]=o.itemIconColorHoverInverted,n["--n-item-icon-color-active"]=o.itemIconColorActiveInverted,n["--n-item-icon-color-active-hover"]=o.itemIconColorActiveHoverInverted,n["--n-item-icon-color-child-active"]=o.itemIconColorChildActiveInverted,n["--n-item-icon-color-child-active-hover"]=o.itemIconColorChildActiveHoverInverted,n["--n-item-icon-color-collapsed"]=o.itemIconColorCollapsedInverted,n["--n-item-text-color-horizontal"]=o.itemTextColorHorizontalInverted,n["--n-item-text-color-hover-horizontal"]=o.itemTextColorHoverHorizontalInverted,n["--n-item-text-color-active-horizontal"]=o.itemTextColorActiveHorizontalInverted,n["--n-item-text-color-child-active-horizontal"]=o.itemTextColorChildActiveHorizontalInverted,n["--n-item-text-color-child-active-hover-horizontal"]=o.itemTextColorChildActiveHoverHorizontalInverted,n["--n-item-text-color-active-hover-horizontal"]=o.itemTextColorActiveHoverHorizontalInverted,n["--n-item-icon-color-horizontal"]=o.itemIconColorHorizontalInverted,n["--n-item-icon-color-hover-horizontal"]=o.itemIconColorHoverHorizontalInverted,n["--n-item-icon-color-active-horizontal"]=o.itemIconColorActiveHorizontalInverted,n["--n-item-icon-color-active-hover-horizontal"]=o.itemIconColorActiveHoverHorizontalInverted,n["--n-item-icon-color-child-active-horizontal"]=o.itemIconColorChildActiveHorizontalInverted,n["--n-item-icon-color-child-active-hover-horizontal"]=o.itemIconColorChildActiveHoverHorizontalInverted,n["--n-arrow-color"]=o.arrowColorInverted,n["--n-arrow-color-hover"]=o.arrowColorHoverInverted,n["--n-arrow-color-active"]=o.arrowColorActiveInverted,n["--n-arrow-color-active-hover"]=o.arrowColorActiveHoverInverted,n["--n-arrow-color-child-active"]=o.arrowColorChildActiveInverted,n["--n-arrow-color-child-active-hover"]=o.arrowColorChildActiveHoverInverted,n["--n-item-color-hover"]=o.itemColorHoverInverted,n["--n-item-color-active"]=o.itemColorActiveInverted,n["--n-item-color-active-hover"]=o.itemColorActiveHoverInverted,n["--n-item-color-active-collapsed"]=o.itemColorActiveCollapsedInverted):(n["--n-group-text-color"]=o.groupTextColor,n["--n-color"]=o.color,n["--n-item-text-color"]=o.itemTextColor,n["--n-item-text-color-hover"]=o.itemTextColorHover,n["--n-item-text-color-active"]=o.itemTextColorActive,n["--n-item-text-color-child-active"]=o.itemTextColorChildActive,n["--n-item-text-color-child-active-hover"]=o.itemTextColorChildActiveHover,n["--n-item-text-color-active-hover"]=o.itemTextColorActiveHover,n["--n-item-icon-color"]=o.itemIconColor,n["--n-item-icon-color-hover"]=o.itemIconColorHover,n["--n-item-icon-color-active"]=o.itemIconColorActive,n["--n-item-icon-color-active-hover"]=o.itemIconColorActiveHover,n["--n-item-icon-color-child-active"]=o.itemIconColorChildActive,n["--n-item-icon-color-child-active-hover"]=o.itemIconColorChildActiveHover,n["--n-item-icon-color-collapsed"]=o.itemIconColorCollapsed,n["--n-item-text-color-horizontal"]=o.itemTextColorHorizontal,n["--n-item-text-color-hover-horizontal"]=o.itemTextColorHoverHorizontal,n["--n-item-text-color-active-horizontal"]=o.itemTextColorActiveHorizontal,n["--n-item-text-color-child-active-horizontal"]=o.itemTextColorChildActiveHorizontal,n["--n-item-text-color-child-active-hover-horizontal"]=o.itemTextColorChildActiveHoverHorizontal,n["--n-item-text-color-active-hover-horizontal"]=o.itemTextColorActiveHoverHorizontal,n["--n-item-icon-color-horizontal"]=o.itemIconColorHorizontal,n["--n-item-icon-color-hover-horizontal"]=o.itemIconColorHoverHorizontal,n["--n-item-icon-color-active-horizontal"]=o.itemIconColorActiveHorizontal,n["--n-item-icon-color-active-hover-horizontal"]=o.itemIconColorActiveHoverHorizontal,n["--n-item-icon-color-child-active-horizontal"]=o.itemIconColorChildActiveHorizontal,n["--n-item-icon-color-child-active-hover-horizontal"]=o.itemIconColorChildActiveHoverHorizontal,n["--n-arrow-color"]=o.arrowColor,n["--n-arrow-color-hover"]=o.arrowColorHover,n["--n-arrow-color-active"]=o.arrowColorActive,n["--n-arrow-color-active-hover"]=o.arrowColorActiveHover,n["--n-arrow-color-child-active"]=o.arrowColorChildActive,n["--n-arrow-color-child-active-hover"]=o.arrowColorChildActiveHover,n["--n-item-color-hover"]=o.itemColorHover,n["--n-item-color-active"]=o.itemColorActive,n["--n-item-color-active-hover"]=o.itemColorActiveHover,n["--n-item-color-active-collapsed"]=o.itemColorActiveCollapsed),n}),z=r?ie("menu",y(()=>e.inverted?"a":"b"),x,e):void 0;return{mergedClsPrefix:t,controlledExpandedKeys:H,uncontrolledExpanededKeys:p,mergedExpandedKeys:A,uncontrolledValue:N,mergedValue:S,activePath:L,tmNodes:O,mergedTheme:i,mergedCollapsed:u,cssVars:r?void 0:x,themeClass:z==null?void 0:z.themeClass,onRender:z==null?void 0:z.onRender,showOption:w}},render(){const{mergedClsPrefix:e,mode:t,themeClass:r,onRender:i}=this;return i==null||i(),s("div",{role:t==="horizontal"?"menubar":"menu",class:[`${e}-menu`,r,`${e}-menu--${t}`,this.mergedCollapsed&&`${e}-menu--collapsed`],style:this.cssVars},this.tmNodes.map(c=>ve(c,this.$props)))}}),yo=P({setup(){},render(){return I(q,{justify:"start",align:"center",class:"h-16 w-48 ml-12"},{default:()=>[I("h2",{class:"text-2xl font-bold"},[te("SeaTunnel")])]})}}),Co=P({setup(){const e=Le({});return De().then(t=>Object.assign(e,t)),{data:e}},render(){return I(q,{justify:"center",align:"center",wrap:!1,class:"h-16 mr-6"},{default:()=>[I("h2",{class:"text-base font-bold"},[te("Version:")]),I("span",{class:"text-base text-nowrap"},[this.data.projectVersion]),I("h2",{class:"text-base font-bold ml-4"},[te("Commit:")]),I("span",{class:"text-base text-nowrap"},[this.data.gitCommitAbbrev])]})}}),zo=P({setup(){},render(){return I(q,{justify:"space-between",class:"h-16 border-gray-200"},{default:()=>[I(q,null,{default:()=>[I(yo,null,null)]}),I(Co,null,null)]})}}),wo=P({name:"Sidebar",props:{sideMenuOptions:{type:Array,default:[]},sideKey:{type:String,default:""}},setup(){je();const e=k(!1),t=[""],r=xe(),{t:i}=Ke.useI18n(),c=k(!1),u=Fe(),h=k(u.getTheme),v=k([{label:()=>s(Z,{to:{path:"/overview"},exact:!1},{default:()=>i("menu.overview")}),key:"overview"},{label:()=>s(Z,{to:{path:"/jobs"},exact:!1},{default:()=>i("menu.jobs")}),key:"jobs"},{label:()=>s(Z,{to:{path:"/managers"},exact:!1},{default:()=>i("menu.managers")}),key:"managers"}]);return Be(()=>{}),{collapsedRef:e,defaultExpandedKeys:t,menuStyle:h,themeStore:u,showDrop:c,sideMenuOptions:v,route:r}},render(){return I(lo,{bordered:!0,nativeScrollbar:!1,"show-trigger":"bar","collapse-mode":"width",collapsed:this.collapsedRef,onCollapse:()=>this.collapsedRef=!0,onExpand:()=>this.collapsedRef=!1,width:196},{default:()=>[I(xo,{class:"tab-vertical",value:this.$props.sideKey,options:this.sideMenuOptions,defaultExpandedKeys:this.defaultExpandedKeys},null)]})}}),No=P({setup(){window.$message=Ue();const e=xe(),t=k(e.fullPath);let r=k(!1);const i=k(e.meta.activeMenu);return _e(()=>e,()=>{var c;r.value=(c=e==null?void 0:e.meta)==null?void 0:c.showSide,i.value=e.meta.activeSide},{immediate:!0,deep:!0}),{showSide:r,menuKey:i,routeKey:t}},render(){return I(fe,null,{default:()=>[I(oo,{bordered:!0},{default:()=>[I(zo,null,null)]}),I(he,{style:{height:"calc(100vh - 65px)"}},{default:()=>[I(fe,{"has-sider":!0,position:"absolute"},{default:()=>[this.showSide&&I(wo,{sideKey:this.menuKey},null),I(he,{"native-scrollbar":!1,style:"padding: 16px 22px 0px 22px",class:"p-16-22-0-22",contentStyle:"height: 100%"},{default:()=>[I(q,{vertical:!0,justify:"space-between",style:"height: 100%",size:"small"},{default:()=>[I(Ve("router-view"),{key:this.routeKey,class:!this.showSide&&"px-32 py-12"},null)]})]})]})]})]})}});export{No as default};
