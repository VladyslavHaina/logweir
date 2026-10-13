import type { ReactNode } from "react";
import { Content, Flex, FlexItem, PageSection, Title } from "@patternfly/react-core";

/** Every page opens the same way: where you are, what this page answers, and its actions. */
export function PageHeader({
  title,
  description,
  status,
  actions,
  breadcrumb,
}: {
  title: ReactNode;
  description?: ReactNode;
  status?: ReactNode;
  actions?: ReactNode;
  breadcrumb?: ReactNode;
}) {
  return (
    <PageSection className="lw-page-header" aria-label="Page header">
      {breadcrumb}
      <Flex
        justifyContent={{ default: "justifyContentSpaceBetween" }}
        alignItems={{ default: "alignItemsFlexStart" }}
        gap={{ default: "gapMd" }}
        flexWrap={{ default: "wrap" }}
      >
        <FlexItem className="lw-page-header__text">
          <Flex alignItems={{ default: "alignItemsCenter" }} gap={{ default: "gapSm" }} flexWrap={{ default: "wrap" }}>
            <Title headingLevel="h1" size="2xl" className="lw-break">
              {title}
            </Title>
            {status}
          </Flex>
          {description && (
            <Content component="p" className="lw-subtle lw-page-header__description">
              {description}
            </Content>
          )}
        </FlexItem>
        {actions && <FlexItem className="lw-page-header__actions">{actions}</FlexItem>}
      </Flex>
    </PageSection>
  );
}
