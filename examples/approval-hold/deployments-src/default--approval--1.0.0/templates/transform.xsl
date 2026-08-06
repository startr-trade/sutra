<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" omit-xml-declaration="yes"/>
  <!-- Demonstrates XSLT input parity: the inbound message stays the SOURCE document (E2EId read via
       local-name(), namespace-agnostic), the decoded payload is the $payload param node-tree (Amount),
       and scalar process variables arrive as string params ($prepped, set by the Prep data task) — the
       same set of inputs the Handlebars engine receives, just in XML form. -->
  <xsl:param name="payload"/>
  <xsl:param name="prepped"/>
  <xsl:template match="/">
    <Xslt prepped="{$prepped}" e2e="{//*[local-name()='E2EId']}" amount="{$payload/Amount}"/>
  </xsl:template>
</xsl:stylesheet>
