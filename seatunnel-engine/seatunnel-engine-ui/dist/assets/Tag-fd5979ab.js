import{bN as ao,bQ as no,bR as o,c as so,a as f,g as x,A as P,i as I,d as to,y as io,r as ho,b as go,u as U,p as Co,t as bo,T as uo,e as F,f as vo,ae as N,h as y,ac as fo,n as po,m as ko,z as mo,V as d,bS as L}from"./index-c41f75db.js";const xo=l=>{const{textColor2:h,primaryColorHover:r,primaryColorPressed:p,primaryColor:a,infoColor:i,successColor:s,warningColor:n,errorColor:t,baseColor:k,borderColor:m,opacityDisabled:g,tagColor:u,closeIconColor:e,closeIconColorHover:c,closeIconColorPressed:v,borderRadiusSmall:C,fontSizeMini:b,fontSizeTiny:z,fontSizeSmall:S,fontSizeMedium:B,heightMini:$,heightTiny:H,heightSmall:R,heightMedium:T,closeColorHover:M,closeColorPressed:E,buttonColor2Hover:w,buttonColor2Pressed:_,fontWeightStrong:O}=l;return Object.assign(Object.assign({},no),{closeBorderRadius:C,heightTiny:$,heightSmall:H,heightMedium:R,heightLarge:T,borderRadius:C,opacityDisabled:g,fontSizeTiny:b,fontSizeSmall:z,fontSizeMedium:S,fontSizeLarge:B,fontWeightStrong:O,textColorCheckable:h,textColorHoverCheckable:h,textColorPressedCheckable:h,textColorChecked:k,colorCheckable:"#0000",colorHoverCheckable:w,colorPressedCheckable:_,colorChecked:a,colorCheckedHover:r,colorCheckedPressed:p,border:`1px solid ${m}`,textColor:h,color:u,colorBordered:"rgb(250, 250, 252)",closeIconColor:e,closeIconColorHover:c,closeIconColorPressed:v,closeColorHover:M,closeColorPressed:E,borderPrimary:`1px solid ${o(a,{alpha:.3})}`,textColorPrimary:a,colorPrimary:o(a,{alpha:.12}),colorBorderedPrimary:o(a,{alpha:.1}),closeIconColorPrimary:a,closeIconColorHoverPrimary:a,closeIconColorPressedPrimary:a,closeColorHoverPrimary:o(a,{alpha:.12}),closeColorPressedPrimary:o(a,{alpha:.18}),borderInfo:`1px solid ${o(i,{alpha:.3})}`,textColorInfo:i,colorInfo:o(i,{alpha:.12}),colorBorderedInfo:o(i,{alpha:.1}),closeIconColorInfo:i,closeIconColorHoverInfo:i,closeIconColorPressedInfo:i,closeColorHoverInfo:o(i,{alpha:.12}),closeColorPressedInfo:o(i,{alpha:.18}),borderSuccess:`1px solid ${o(s,{alpha:.3})}`,textColorSuccess:s,colorSuccess:o(s,{alpha:.12}),colorBorderedSuccess:o(s,{alpha:.1}),closeIconColorSuccess:s,closeIconColorHoverSuccess:s,closeIconColorPressedSuccess:s,closeColorHoverSuccess:o(s,{alpha:.12}),closeColorPressedSuccess:o(s,{alpha:.18}),borderWarning:`1px solid ${o(n,{alpha:.35})}`,textColorWarning:n,colorWarning:o(n,{alpha:.15}),colorBorderedWarning:o(n,{alpha:.12}),closeIconColorWarning:n,closeIconColorHoverWarning:n,closeIconColorPressedWarning:n,closeColorHoverWarning:o(n,{alpha:.12}),closeColorPressedWarning:o(n,{alpha:.18}),borderError:`1px solid ${o(t,{alpha:.23})}`,textColorError:t,colorError:o(t,{alpha:.1}),colorBorderedError:o(t,{alpha:.08}),closeIconColorError:t,closeIconColorHoverError:t,closeIconColorPressedError:t,closeColorHoverError:o(t,{alpha:.12}),closeColorPressedError:o(t,{alpha:.18})})},yo={name:"Tag",common:ao,self:xo},Po=yo,Io={color:Object,type:{type:String,default:"default"},round:Boolean,size:{type:String,default:"medium"},closable:Boolean,disabled:{type:Boolean,default:void 0}},zo=so("tag",`
 white-space: nowrap;
 position: relative;
 box-sizing: border-box;
 cursor: default;
 display: inline-flex;
 align-items: center;
 flex-wrap: nowrap;
 padding: var(--n-padding);
 border-radius: var(--n-border-radius);
 color: var(--n-text-color);
 background-color: var(--n-color);
 transition: 
 border-color .3s var(--n-bezier),
 background-color .3s var(--n-bezier),
 color .3s var(--n-bezier),
 box-shadow .3s var(--n-bezier),
 opacity .3s var(--n-bezier);
 line-height: 1;
 height: var(--n-height);
 font-size: var(--n-font-size);
`,[f("strong",`
 font-weight: var(--n-font-weight-strong);
 `),x("border",`
 pointer-events: none;
 position: absolute;
 left: 0;
 right: 0;
 top: 0;
 bottom: 0;
 border-radius: inherit;
 border: var(--n-border);
 transition: border-color .3s var(--n-bezier);
 `),x("icon",`
 display: flex;
 margin: 0 4px 0 0;
 color: var(--n-text-color);
 transition: color .3s var(--n-bezier);
 font-size: var(--n-avatar-size-override);
 `),x("avatar",`
 display: flex;
 margin: 0 6px 0 0;
 `),x("close",`
 margin: var(--n-close-margin);
 transition:
 background-color .3s var(--n-bezier),
 color .3s var(--n-bezier);
 `),f("round",`
 padding: 0 calc(var(--n-height) / 3);
 border-radius: calc(var(--n-height) / 2);
 `,[x("icon",`
 margin: 0 4px 0 calc((var(--n-height) - 8px) / -2);
 `),x("avatar",`
 margin: 0 6px 0 calc((var(--n-height) - 8px) / -2);
 `),f("closable",`
 padding: 0 calc(var(--n-height) / 4) 0 calc(var(--n-height) / 3);
 `)]),f("icon, avatar",[f("round",`
 padding: 0 calc(var(--n-height) / 3) 0 calc(var(--n-height) / 2);
 `)]),f("disabled",`
 cursor: not-allowed !important;
 opacity: var(--n-opacity-disabled);
 `),f("checkable",`
 cursor: pointer;
 box-shadow: none;
 color: var(--n-text-color-checkable);
 background-color: var(--n-color-checkable);
 `,[P("disabled",[I("&:hover","background-color: var(--n-color-hover-checkable);",[P("checked","color: var(--n-text-color-hover-checkable);")]),I("&:active","background-color: var(--n-color-pressed-checkable);",[P("checked","color: var(--n-text-color-pressed-checkable);")])]),f("checked",`
 color: var(--n-text-color-checked);
 background-color: var(--n-color-checked);
 `,[P("disabled",[I("&:hover","background-color: var(--n-color-checked-hover);"),I("&:active","background-color: var(--n-color-checked-pressed);")])])])]),So=Object.assign(Object.assign(Object.assign({},U.props),Io),{bordered:{type:Boolean,default:void 0},checked:Boolean,checkable:Boolean,strong:Boolean,triggerClickOnClose:Boolean,onClose:[Array,Function],onMouseenter:Function,onMouseleave:Function,"onUpdate:checked":Function,onUpdateChecked:Function,internalCloseFocusable:{type:Boolean,default:!0},internalCloseIsButtonTag:{type:Boolean,default:!0},onCheckedChange:Function}),Bo=po("n-tag"),Ho=to({name:"Tag",props:So,setup(l){io(()=>{l.onCheckedChange!==void 0&&mo("tag","`on-checked-change` is deprecated, please use `on-update:checked` instead")});const h=ho(null),{mergedBorderedRef:r,mergedClsPrefixRef:p,inlineThemeDisabled:a,mergedRtlRef:i}=go(l),s=U("Tag","-tag",zo,Po,l,p);Co(Bo,{roundRef:bo(l,"round")});function n(e){if(!l.disabled&&l.checkable){const{checked:c,onCheckedChange:v,onUpdateChecked:C,"onUpdate:checked":b}=l;C&&C(!c),b&&b(!c),v&&v(!c)}}function t(e){if(l.triggerClickOnClose||e.stopPropagation(),!l.disabled){const{onClose:c}=l;c&&ko(c,e)}}const k={setTextContent(e){const{value:c}=h;c&&(c.textContent=e)}},m=uo("Tag",i,p),g=F(()=>{const{type:e,size:c,color:{color:v,textColor:C}={}}=l,{common:{cubicBezierEaseInOut:b},self:{padding:z,closeMargin:S,closeMarginRtl:B,borderRadius:$,opacityDisabled:H,textColorCheckable:R,textColorHoverCheckable:T,textColorPressedCheckable:M,textColorChecked:E,colorCheckable:w,colorHoverCheckable:_,colorPressedCheckable:O,colorChecked:V,colorCheckedHover:D,colorCheckedPressed:K,closeBorderRadius:A,fontWeightStrong:Q,[d("colorBordered",e)]:q,[d("closeSize",c)]:G,[d("closeIconSize",c)]:J,[d("fontSize",c)]:X,[d("height",c)]:W,[d("color",e)]:Y,[d("textColor",e)]:Z,[d("border",e)]:oo,[d("closeIconColor",e)]:j,[d("closeIconColorHover",e)]:eo,[d("closeIconColorPressed",e)]:ro,[d("closeColorHover",e)]:lo,[d("closeColorPressed",e)]:co}}=s.value;return{"--n-font-weight-strong":Q,"--n-avatar-size-override":`calc(${W} - 8px)`,"--n-bezier":b,"--n-border-radius":$,"--n-border":oo,"--n-close-icon-size":J,"--n-close-color-pressed":co,"--n-close-color-hover":lo,"--n-close-border-radius":A,"--n-close-icon-color":j,"--n-close-icon-color-hover":eo,"--n-close-icon-color-pressed":ro,"--n-close-icon-color-disabled":j,"--n-close-margin":S,"--n-close-margin-rtl":B,"--n-close-size":G,"--n-color":v||(r.value?q:Y),"--n-color-checkable":w,"--n-color-checked":V,"--n-color-checked-hover":D,"--n-color-checked-pressed":K,"--n-color-hover-checkable":_,"--n-color-pressed-checkable":O,"--n-font-size":X,"--n-height":W,"--n-opacity-disabled":H,"--n-padding":z,"--n-text-color":C||Z,"--n-text-color-checkable":R,"--n-text-color-checked":E,"--n-text-color-hover-checkable":T,"--n-text-color-pressed-checkable":M}}),u=a?vo("tag",F(()=>{let e="";const{type:c,size:v,color:{color:C,textColor:b}={}}=l;return e+=c[0],e+=v[0],C&&(e+=`a${L(C)}`),b&&(e+=`b${L(b)}`),r.value&&(e+="c"),e}),g,l):void 0;return Object.assign(Object.assign({},k),{rtlEnabled:m,mergedClsPrefix:p,contentRef:h,mergedBordered:r,handleClick:n,handleCloseClick:t,cssVars:a?void 0:g,themeClass:u==null?void 0:u.themeClass,onRender:u==null?void 0:u.onRender})},render(){var l,h;const{mergedClsPrefix:r,rtlEnabled:p,closable:a,color:{borderColor:i}={},round:s,onRender:n,$slots:t}=this;n==null||n();const k=N(t.avatar,g=>g&&y("div",{class:`${r}-tag__avatar`},g)),m=N(t.icon,g=>g&&y("div",{class:`${r}-tag__icon`},g));return y("div",{class:[`${r}-tag`,this.themeClass,{[`${r}-tag--rtl`]:p,[`${r}-tag--strong`]:this.strong,[`${r}-tag--disabled`]:this.disabled,[`${r}-tag--checkable`]:this.checkable,[`${r}-tag--checked`]:this.checkable&&this.checked,[`${r}-tag--round`]:s,[`${r}-tag--avatar`]:k,[`${r}-tag--icon`]:m,[`${r}-tag--closable`]:a}],style:this.cssVars,onClick:this.handleClick,onMouseenter:this.onMouseenter,onMouseleave:this.onMouseleave},m||k,y("span",{class:`${r}-tag__content`,ref:"contentRef"},(h=(l=this.$slots).default)===null||h===void 0?void 0:h.call(l)),!this.checkable&&a?y(fo,{clsPrefix:r,class:`${r}-tag__close`,disabled:this.disabled,onClick:this.handleCloseClick,focusable:this.internalCloseFocusable,round:s,isButtonTag:this.internalCloseIsButtonTag,absolute:!0}):null,!this.checkable&&this.mergedBordered?y("div",{class:`${r}-tag__border`,style:{borderColor:i}}):null)}});export{Ho as N};
