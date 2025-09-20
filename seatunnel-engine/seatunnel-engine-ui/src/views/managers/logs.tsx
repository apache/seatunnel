import NodeLog from "@/components/node-log";
import { NLayout, NLayoutContent } from "naive-ui";
import { defineComponent } from "vue";

export default defineComponent({
    setup() {
    return () => (
      <NLayout>
        <NLayoutContent>
          <NodeLog></NodeLog>
        </NLayoutContent>
      </NLayout>
    )
    }
})