# STRAKE DATA
## SEO / AEO Implementation Plan

Complete Roadmap for Search & AI Ranking — March 2026

*strakedata.com*

---

## Strategic Context: Why This Moment Matters

Strake sits at the convergence of three fast-moving trends: the MCP protocol becoming the default AI data-access standard (97M monthly SDK downloads as of early 2026), the Open Semantic Interchange (OSI) v1.0 spec shipping January 27 2026 with zero tooling yet in market, and Google's AI search features shifting from supplementary to primary — with AI Overview citations now delivering 35% more organic clicks than non-cited results on the same query.

The strategy below is built on one asymmetric insight: Strake is the only infrastructure company that operates at the execution layer — the sandboxed runtime where AI agents actually run SQL and Python against enterprise data. Every competitor (BI tools, NL2SQL, connector marketplaces, semantic layer vendors) operates above or beside this layer. No one else has written the definitive content for this intersection. That window is open now, measured in weeks for some categories, months for others.

&gt; **The core thesis in one sentence:**
&gt; 
&gt; Strake does not need to compete for existing search categories. It needs to define and own new ones before Snowflake, Databricks, or a well-funded startup does it first.

---

## Keyword Architecture

Three tiers of target terms, each requiring a different content type and timeline.

| Tier | Cluster | Competition | Window | Priority |
|------|---------|-------------|--------|----------|
| **Own Now** | AI data layer, agentic data runtime, MCP data server, AI agent Snowflake access, Python sandbox data analysis | Very Low | Weeks | Immediate |
| **Build Authority** | Federated SQL for AI, secure AI data access enterprise, data warehouse AI agent, MCP enterprise security | Medium | 3–6 months | Q2 2026 |
| **Breakout Bets** | OSI semantic layer runtime, MCP streamable HTTP production, A2A enterprise data, agentic AI foundation | Near Zero | 6–12 months | Q3 2026 |
| **Ecosystem** | Strake vs NL2SQL, AI agent RBAC, prompt injection prevention data, zero-copy Arrow Python | Low–Medium | Ongoing | Parallel |

---

## PHASE 1: Foundation & Category Definition
### Weeks 1–4 · Immediate

Phase 1 is about claiming territory before anyone else does. Every item here targets a keyword cluster with currently low competition but rapidly rising search intent. None of these require waiting for product features — all can be published using existing Strake architecture documentation.

---

### 1.1 Define the Category: 'What Is an AI Data Layer?'

**Why this is the highest-leverage action in the entire plan**

AI search engines — Perplexity, ChatGPT Search, Google AI Mode — answer definitional questions by citing whoever gave the clearest, most structured first-sentence answer. No page currently owns the definition of 'AI Data Layer'. Strake named the category. Strake should be the entity Google's knowledge graph associates with it. This page, done correctly, will be cited in AI-generated answers for 12–24 months.

**Page specification**

- **URL:** strakedata.com/what-is-an-ai-data-layer
- **Title tag:** What Is an AI Data Layer? | Strake
- **Meta description (≤155 chars):** An AI data layer is the execution infrastructure that gives AI agents sandboxed, federated access to enterprise data — without moving data or giving agents write permissions.
- **H1:** What Is an AI Data Layer?
- **Opening paragraph (first 70 words, written for AI extraction):** 
  &gt; 'An AI data layer is the runtime infrastructure that gives AI agents safe, high-performance access to an enterprise data estate — spanning SQL warehouses, object storage, and APIs — without data movement, without INSERT/UPDATE permissions, and without context overflow. Strake is an AI data layer: a federated SQL engine with Firecracker MicroVM-isolated Python sandboxes, designed as infrastructure for agentic workflows, not as a GUI or NL2SQL translator.'

**Content structure (H2 sections)**

- Why AI agents need a dedicated data layer (problem definition)
- How an AI data layer differs from an API connector (direct differentiation)
- How an AI data layer differs from NL2SQL (second differentiation)
- The four components of an AI data layer: execution, federation, discovery, security
- Who needs an AI data layer (ICP definition: enterprises with AI agents, sensitive data, and multi-source warehouses)
- FAQ section (FAQPage schema — see technical section below)

#### &gt;&gt; TASKS: Page 1.1 — AI Data Layer Definition

- [ ] Write the 70-word opening paragraph exactly as quoted above — this is the AI extraction target
- [ ] Implement FAQPage JSON-LD schema with 8 questions (see schema section)
- [ ] Implement SoftwareApplication schema on the same page linking to GitHub and Crunchbase
- [ ] Add Person schema for the author (founding engineer) with sameAs linking to GitHub profile
- [ ] Submit URL to Google Search Console for immediate indexing via URL Inspection Tool
- [ ] Allow GPTBot, ClaudeBot, PerplexityBot, Google-Extended in robots.txt (verify now — many infra sites block by default)
- [ ] Internal link from every other Strake page to this URL using exact anchor text 'AI data layer'

---

### 1.2 MCP Enterprise Security: The Production Checklist

**Why this is urgent**

43% of publicly available MCP servers contain command injection vulnerabilities (Invariant Labs research, 2025). MCP has crossed 97 million monthly SDK downloads. Enterprises are deploying it right now without a security standard. No authoritative production checklist exists from an infrastructure vendor. Strake's SQL exfiltration prevention, prompt injection detection, and sandbox isolation are the exact answer to this problem — and there is zero competition for this content cluster today.

**Page specification**

- **URL:** strakedata.com/mcp-enterprise-security
- **Title:** MCP Enterprise Security: The Production Checklist for Safe Deployment
- **Target queries:** 'MCP security enterprise', 'secure MCP server', 'MCP production security', 'MCP command injection prevention'
- **Lead sentence (AI extraction target):** 'Deploying MCP in an enterprise environment requires security controls at three layers: network isolation, application-level prompt injection detection, and runtime sandboxing — because 43% of publicly available MCP servers currently contain command injection vulnerabilities.'

**Content structure**

- The threat model: what can go wrong when an MCP server has warehouse access (prompt injection, SQL exfiltration, session poisoning, context overflow)
- Layer 1 — Network: air-gapping, CLONE_NEWNET, what hardware enforcement means vs. software isolation
- Layer 2 — Application: prompt injection detection patterns, DLP scanning, taint tracking
- Layer 3 — Runtime: why Firecracker MicroVMs provide structural (not probabilistic) guarantees that containers do not
- The production checklist: 12 items with pass/fail criteria (this becomes a citable, shareable asset)
- How Strake implements all 12 items (honest, technical, not marketing copy)

&gt; **Content strategy note:**
&gt; 
&gt; The 43% vulnerability statistic from Invariant Labs should be cited in the opening paragraph with a hyperlink. AI search systems that crawl this page will associate Strake's content with this verified data point, which is what makes it citation-worthy rather than another vendor blog post.

#### &gt;&gt; TASKS: Page 1.2 — MCP Security Checklist

- [ ] Research and document Strake's actual SQL exfiltration prevention implementation for the checklist (makes content authentic and uncopyable)
- [ ] Format the 12-item checklist as a proper HowTo schema block (ordered list, each item is a HowToStep)
- [ ] Publish the checklist as a standalone downloadable PDF — creates a backlink target when others cite it
- [ ] Pitch the checklist to the MCP Discord / Slack community for organic distribution
- [ ] Submit to Hacker News as a 'resource' post, not a product post — lead with the vulnerability data, not Strake
- [ ] Create a GitHub Gist version of the checklist to earn developer community citations

---

### 1.3 MCP Streamable HTTP at Production Scale

**Why the correct framing matters here**

CORRECTION from the feedback document: MCP Streamable HTTP itself is not an emerging topic — it shipped in early 2025 and is well-documented. The real content gap is at the layer above it: the production engineering problems that emerge when running Streamable HTTP at enterprise scale. The MCP maintainers published this as their #1 2026 engineering priority just days ago. No vendor has written the definitive guide yet.

**The real content gap: the three unsolved production problems**

- Stateful sessions vs. load balancers: how to maintain session continuity across horizontally scaled MCP server instances
- Long-running data queries: how MCP Tasks handles async operations and what happens when a query takes 45 seconds to execute across a federated source
- Registry discoverability: the absence of a standard way for a gateway to discover what an MCP server can do without connecting to it

**Page specification**

- **URL:** strakedata.com/mcp-streamable-http-production
- **Title:** MCP Streamable HTTP at Scale: Session Management, Horizontal Scaling, and Long-Running Queries
- **Target queries:** 'MCP streamable HTTP scaling', 'MCP session management production', 'MCP long-running queries', 'MCP horizontal scaling'
- **Note:** This content is based on Strake's direct Q1 2026 engineering work on MCP protocol modernization — first-person engineering narrative, not marketing

#### &gt;&gt; TASKS: Page 1.3 — MCP Streamable HTTP Production

- [ ] Write this as an engineering blog post with the founding engineer as the byline — not a product marketing page
- [ ] Include actual code examples showing how Strake handles stateful session routing across load-balanced instances
- [ ] Benchmark the latency difference between standard HTTP and Streamable HTTP for 30-second federated queries
- [ ] Submit as a Show HN post — the production scaling angle is a genuine engineering story, not a product pitch
- [ ] Cross-link to the MCP Enterprise Security page (creates internal link cluster around MCP)
- [ ] Add TechArticle schema with datePublished, author Person schema, and mentions property referencing MCP specification

---

## PHASE 2: Technical Authority & AEO Content
### Weeks 5–12 · Q2 2026

Phase 2 builds the content cluster that surrounds the Phase 1 definitional pages. These posts serve dual roles: they earn Google authority through depth and uniqueness, and they give AI search engines a rich body of citations to draw from when answering technical questions about AI agent data infrastructure.

---

### 2.1 OSI v1.0 and AI Agent Execution: The Missing Layer

**Why this is a genuine first-mover window**

The Open Semantic Interchange v1.0 specification shipped January 27, 2026. The coalition behind it includes Snowflake, dbt Labs, Cube, Hex, Salesforce, and Collibra — all of whom will publish their own OSI content from a semantic layer perspective. Strake's unique angle — which nobody else can write — is the execution layer perspective: once you have an OSI-standard metric definition, who actually runs it securely against a federated data source? That is Strake's position in the stack, and it has zero competition right now.

&gt; **Important framing correction:**
&gt; 
&gt; Do NOT compete for 'OSI semantic layer' — Snowflake and dbt Labs already own that cluster. The target is 'how AI agents execute OSI-defined metrics' and 'OSI metric runtime security' — queries that only make sense once you understand there is an execution layer separate from the semantic definition layer. Strake defines this sub-category.

**Content structure**

- What OSI actually defines (metric definitions, dimensions, filters, lineage) — factual summary, not opinion
- The gap OSI does not solve: metric definitions are not the same as metric execution
- What a secure OSI execution runtime needs: federation, sandboxing, read-only enforcement, schema discovery
- How Strake's strake.define_metric() RFC maps to OSI's data model (Phase 2 product roadmap item)
- The three patterns for OSI import/export in an AI agent workflow (import from dbt/Cube, execute in Strake, export certified results back to catalog)

#### &gt;&gt; TASKS: Page 2.1 — OSI Execution Runtime

- [ ] Publish within 60 days of the OSI spec publication (by late March 2026) to be early in the indexing window
- [ ] Reach out to the OSI working group for a quote or co-publish opportunity — even one sentence from a named OSI contributor transforms the content from vendor blog to authoritative reference
- [ ] Add the page to OSI-related conversations in the dbt Slack community and Atlan community forums
- [ ] Implement mentions schema property referencing the official OSI spec URL — signals topical authority to Google's knowledge graph
- [ ] Write a second, shorter post: 'OSI v1.0: What it means for teams running AI agents on enterprise data' (buyer-level, not engineer-level) targeting a different query intent

---

### 2.2 Firecracker MicroVM Isolation for Python Sandboxes

**Why this is a durable SEO moat**

This is the content that no marketing team will write, and no competitor using container-based isolation can honestly publish. Strake's Firecracker implementation is a genuine technical differentiator with verifiable architectural claims. Content at this depth earns backlinks from engineering blogs, gets cited in AI answers to security questions, and signals E-E-A-T in a way that promotional content never can.

**Content structure**

- What Firecracker MicroVM isolation actually means: hardware-enforced boundaries vs. software-enforced containers
- The specific threat model for AI Python sandbox execution (what gVisor gets right, what containers get wrong, why hardware boundaries matter for agentic code execution)
- Strake's implementation: CLONE_NEWNET for network air-gapping, Linux Landlock for filesystem, Seccomp profile specifics
- Honest documentation of probabilistic vs. structural guarantees — what Strake can and cannot guarantee (this honesty is itself an E-E-A-T signal)
- Benchmark: startup latency for Firecracker MicroVM vs. container vs. subprocess for a representative AI data analysis task

**Secondary content from this topic**

- 'Why Containers Are Not Enough for AI Agent Sandboxing' — broader, more search-optimised version of the same argument
- 'The Seccomp Profile for AI Data Agents: A Reference Implementation' — GitHub-hosted, generates backlinks

#### &gt;&gt; TASKS: Page 2.2 — Firecracker Sandbox Deep Dive

- [ ] Publish the Seccomp profile as an open-source GitHub resource with a README that links back to the blog post
- [ ] Submit to the Firecracker community forum / GitHub Discussions — earns a highly credible backlink and developer citations
- [ ] Include actual benchmarks (even on representative workloads) — benchmarks are citable, shareable, and resist being paraphrased by AI systems (which prefers to cite the original data source)
- [ ] Tag the post with the founding engineer's author schema — this is the highest E-E-A-T content Strake will produce

---

### 2.3 MCP + A2A: The Two-Protocol Stack for Enterprise Data Agents

**Why this is a new opportunity not in the original strategy**

The Agentic AI Foundation was launched in December 2025 under the Linux Foundation, with OpenAI, Anthropic, Google, Microsoft, AWS, and Block as co-founders. A2A (Agent-to-Agent protocol) governs how orchestrator agents invoke sub-agents. MCP governs how sub-agents access tools and data. Strake sits at the MCP data execution layer, directly below A2A orchestration. No infrastructure company has yet written the definitive explainer of how these two protocols interact in a production enterprise deployment. This is a zero-competition content gap with high long-term authority.

**The query this answers**

&gt; **Target query:**
&gt; 
&gt; An enterprise architect asks: 'I have an A2A orchestration layer. My agents need to query our Snowflake warehouse and our S3 lake. How do MCP and A2A fit together, and what handles the security boundary?' No current page answers this well. Strake's page will be the canonical answer.

**Content structure**

- What A2A is and what MCP is — crisp, citation-ready definitions (40 words each)
- The two-layer architecture: A2A for agent orchestration, MCP for data tool execution
- Where the security boundary lives: why the MCP data server (Strake) must be sandboxed independently of A2A trust
- The session handoff pattern: how an A2A orchestrator invokes Strake, maintains session context, and receives query results
- Production deployment diagram: A2A orchestrator → MCP gateway → Strake sandbox → federated data sources

#### &gt;&gt; TASKS: Page 2.3 — MCP + A2A Protocol Stack

- [ ] Publish before any of the Agentic AI Foundation members publish tutorials (they will eventually — the window is 2–4 months)
- [ ] Post the architecture diagram to the A2A GitHub Discussions and the Model Context Protocol community — earns early citations
- [ ] Reach out to the Linux Foundation Agentic AI Foundation for a mention in their developer resources page
- [ ] Use this post as the basis for a conference talk proposal to a developer infrastructure event (QCon, SREcon, or the Agentic AI Foundation's own events)

---

### 2.4 Competitive Differentiation Hub: The Comparison Page

**Why one hub beats multiple pages for Strake's stage**

At pre-revenue with a lean team, maintaining 8 individual comparison pages is unrealistic. A single well-structured comparison hub targets all 'Strake vs X' and 'alternative to X' query variants with one piece of content. AI search engines specifically favour comparison pages because they answer 'which tool should I use' questions directly — these queries are categorised as 'exploration' in Google's AI Mode query taxonomy and receive fan-out treatment (multiple sub-queries, multiple citations).

**Comparison matrix to include**

| Compared to | Their approach | The key limitation | Strake's angle |
|-------------|---------------|-------------------|----------------|
| **MooseStack / Moose** | Constrained metric models (TypeScript), ClickHouse-only | No federation, no runtime isolation, no enterprise security | Federated execution across any source, with hardware isolation |
| **Cube.js / Cube** | Semantic layer, pre-aggregations, REST/GraphQL API | Not a runtime — no code execution, no Python, no agent-native interface | Strake is the execution layer Cube's definitions run against |
| **Wren AI / Defog** | NL2SQL — natural language to SQL translation | Agents can 'vibe-SQL' inconsistent definitions; no security sandbox | Agents write SQL; Strake provides the secure runtime and schema discovery |
| **Trino / Presto** | Distributed SQL query engine, developer tool | No agent-native interface, no Python sandbox, no security model for LLM access | Built for AI agents, not human SQL engineers |
| **Generic MCP servers** | Tool-specific connectors (e.g., Snowflake MCP server) | One connector per source, no federation, no isolation, no Python | Single federated runtime across all sources with security guarantees |

#### &gt;&gt; TASKS: Page 2.4 — Comparison Hub

- [ ] **URL:** strakedata.com/vs — clean, indexable, easy to extend
- [ ] Open each comparison section with a one-sentence summary that functions as an AI extraction target
- [ ] Add FAQPage schema covering the top comparison questions (e.g., 'Is Strake a replacement for Cube?', 'How does Strake differ from a standard MCP server?')
- [ ] Ensure each compared product is referenced by its exact official name (for entity graph association)
- [ ] Update the page with each new competitor that enters the space — freshness signals matter for AI citation

---

## PHASE 3: Product-Led Content & Connector Pages
### Q3 2026

Phase 3 content is gated on Phase 2 product roadmap items (strake.define_metric() RFC, Python UDFs, stateful sessions). **Do not publish these pages before the product capability exists** — AI search engines now frequently verify claims against multiple sources, and publishing claims you cannot demonstrate hurts credibility.

---

### 3.1 Runtime-Framed Connector Pages

&gt; **Critical framing note:**
&gt; 
&gt; These are NOT connector listing pages. Do not title them 'Strake Snowflake Connector' — that signals a connector marketplace and contradicts Strake's positioning. Frame each as a runtime and execution story. Title format: 'Running AI agents against [Source] securely with Strake'.

**Priority pages (in order of search volume)**

- 'Running AI agents against Snowflake securely with Strake' — targets 'AI agent Snowflake access', 'Snowflake AI agent security'
- 'Querying Apache Iceberg with AI agents: Strake's federation engine' — targets 'Iceberg AI agent', 'Iceberg REST catalog AI'
- 'AI agent access to PostgreSQL: sandboxed execution with Strake' — targets 'PostgreSQL AI agent access', 'AI agent PostgreSQL security'
- 'S3 and Iceberg lake queries for AI agents' — targets 'AI agent S3 query', 'object storage AI agent'

**Page structure (same for all)**

- Opening: the security problem this source creates for AI agents (specific to that source's data model)
- How Strake's federation engine connects to this source (technical, with schema discovery explanation)
- The sandbox isolation story: what happens if an agent misbehaves against this source
- Code example: strake.search() discovering schema, then an agent SQL query, then a Python analysis
- FAQ: 'Can Strake write to Snowflake?' (No — read-only by design), 'Does Strake move data?' (No — federation, not ETL)

#### &gt;&gt; TASKS: Phase 3 — Connector Runtime Pages

- [ ] Start with the Snowflake page — highest enterprise search volume and most relevant to ICP
- [ ] Do not create the Iceberg or S3 pages until Iceberg REST support is fully hardened (Q1 2026 roadmap item)
- [ ] Each page must include a working code example that can be copy-pasted — this drives developer bookmarking and return visits
- [ ] Add product markup (SoftwareApplication) and HowTo schema to each page
- [ ] Internal link structure: each connector page links to the Security page, the Federation Architecture page, and the AI Data Layer definition page

---

### 3.2 Metric Layer Content: strake.define_metric()

This content is gated on the Phase 2 product RFC. Do not publish until the API design is stable enough to document honestly. When it ships, publish two pieces:

- **Technical:** 'Introducing strake.define_metric(): governed metric definitions for AI agent workflows' — targets developers who have read the MooseStack post
- **Strategic:** 'From exploration SQL to certified metrics: how Strake's session model prevents AI hallucinated business logic' — targets data platform leads and CISOs

Both pieces should reference the OSI specification directly — positioning Strake's metric layer as OSI-compatible is the strategic differentiator that no existing semantic layer vendor (who owns the definition layer) and no connector vendor (who owns integration) can credibly claim.

---

## PHASE 4: Ecosystem, Backlinks & Community Authority
### Ongoing · Q2–Q4 2026

---

### 4.1 Backlink Strategy: Credibility Over Volume

Ten links from the right sources are worth more than 500 directory submissions. At Strake's pre-revenue stage, every backlink acquisition should come from genuine value creation, not outreach campaigns.

| Target | Why it matters | How to earn it | Timeline |
|--------|---------------|----------------|----------|
| **Apache Arrow community / blog** | Highest DA in Strake's technical stack; signals zero-copy execution authority | Write a technical post about zero-copy PyArrow memory pointers in AI agent workloads; submit to Arrow mailing list | Q2 2026 |
| **DataFusion project docs** | Direct technology dependency — 'built with DataFusion' mentions are earned by contributing to docs or filing thoughtful issues | PR to DataFusion docs adding Strake to the 'Projects using DataFusion' list; contribute a small docs fix | Q2 2026 |
| **LanceDB blog** | Strake uses LanceDB for semantic schema discovery — co-publish an engineering post about vector search over enterprise schemas | Reach out to LanceDB team for a co-authored post | Q2–Q3 2026 |
| **OSI working group member page** | Being listed as a member organisation generates a permanent credibility backlink from a high-DA domain with a growing community | Join the working group; contribute to the spec discussion | Immediate |
| **dbt community blog** | The data engineering community Strake's ICP lives in; OSI angle is a natural fit | Pitch a post: 'What OSI means for teams running AI agents against dbt-defined metrics' | Q3 2026 |
| **Hacker News (Show HN)** | Show HN posts generate lasting developer blog backlinks when the technical story is genuine | Each major architecture decision (Firecracker isolation, zero-copy Arrow, MCP Task implementation) deserves its own Show HN | Each phase |
| **The Pragmatic Engineer newsletter** | Gergely's audience is senior engineers at exactly the right companies; Strake's architecture story is his audience | Cold pitch with the Firecracker + agentic data angle; offer an exclusive technical briefing | Q2–Q3 2026 |

---

### 4.2 Developer Community Distribution

Developer discovery for infrastructure tools does not happen through search alone. It happens in communities where engineers share problems and solutions. Strake's content distribution should systematically reach these channels.

- **Model Context Protocol Discord / GitHub Discussions** — Every MCP-related post Strake publishes should be linked here with a genuine contribution to an existing thread, not a promotional drop
- **Data Engineering Discord (600k+ members)** — OSI and semantic layer content fits naturally in #semantic-layer and #ai-tools channels
- **dbt Community Slack** — The OSI and metric layer content has a natural audience in #data-products and #governance channels
- **r/MachineLearning and r/datascience** — Firecracker sandbox post and A2A+MCP architecture post are genuinely novel technical content for these audiences
- **LinkedIn technical posts (founding team)** — Infrastructure decisions with concrete reasoning get organic reach with data engineering leaders; publish 2–3 posts per major technical decision, not product announcements

&gt; **Distribution rule:**
&gt; 
&gt; Never lead with 'we built X'. Lead with 'here is the problem, here is what we learned, here is how we solved it'. The product is the evidence, not the story.

---

## Technical SEO Implementation

---

### 5.1 Schema Markup Priority Stack

Schema markup is the structured data layer that bridges traditional search indexing and AI knowledge graph association. Implement in this priority order:

#### 1. Organization schema — sitewide, highest priority

**Implement in `&lt;head&gt;` on every page via JSON-LD:**

```json
{
  "@context": "https://schema.org",
  "@type": "Organization",
  "name": "Strake",
  "url": "https://www.strakedata.com",
  "logo": "https://www.strakedata.com/logo.png",
  "sameAs": [
    "https://github.com/strakedata",
    "https://www.linkedin.com/company/strakedata",
    "https://www.crunchbase.com/organization/strake"
  ],
  "description": "Strake is the AI Data Layer — a federated SQL engine with Firecracker MicroVM-isolated Python sandboxes for enterprise AI agent data workflows."
}
```

#### 2. SoftwareApplication schema — homepage + product pages
Signals to Google's knowledge graph that Strake is a software product with a specific category and feature set. Required fields: applicationCategory ('BusinessApplication'), operatingSystem ('Linux, macOS'), offers (free tier description), featureList.
#### 3. FAQPage schema — homepage, all landing pages, comparison hub
The most impactful schema for AI Overview citations. Each FAQ must have a Question and an Answer that can be read as a standalone sentence — not a fragment. Write the answers as AI extraction targets: first sentence answers the question completely, second sentence adds context.
| Page                      | FAQ questions to include                                                                                                                                                                           |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Homepage                  | What is an AI data layer? \| Is Strake read-only? \| What databases does Strake support? \| How is Strake different from NL2SQL? \| Does Strake move data?                                         |
| /mcp-enterprise-security  | Is MCP secure for enterprise use? \| What is the most common MCP security vulnerability? \| How does Strake prevent SQL injection from AI agents? \| What is taint tracking in an AI data runtime? |
| /what-is-an-ai-data-layer | What is an AI data layer? \| Why do AI agents need a data runtime, not just an API? \| What is the difference between an AI data layer and a connector marketplace? \| Who needs an AI data layer? |
| /vs (comparison hub)      | Is Strake a replacement for Cube? \| How does Strake compare to Wren AI? \| Can Strake be used alongside dbt? \| Is Strake an MCP server?                                                          |

#### 4. TechArticle schema — all engineering blog posts
Required fields: headline, author (Person type, with sameAs linking to GitHub), datePublished, dateModified, description, keywords, mentions (listing Apache Arrow, DataFusion, LanceDB, Firecracker, Apache Iceberg as SoftwareApplication entities).
#### 5. HowTo schema — tutorial and implementation guide posts
The MCP Security Checklist, connector implementation guides, and the A2A+MCP deployment guide should all use HowTo schema. Each step must be a HowToStep with a standalone description. This is what triggers 'how to' featured snippets and step-by-step AI Mode answers.
#### 5.2 robots.txt — Immediate Action Required
Check this today:
Many infrastructure and developer tools companies have their robots.txt blocking AI crawlers by default — either via an overly broad Disallow rule or by explicitly blocking GPTBot. This is invisible self-harm for AEO. Verify and explicitly allow the following crawlers in robots.txt:
```robots.txt
User-agent: GPTBot
Allow: /

User-agent: ClaudeBot
Allow: /

User-agent: PerplexityBot
Allow: /

User-agent: Google-Extended
Allow: /

User-agent: ChatGPT-User
Allow: /
```
#### 5.3 Site Architecture
The recommended URL architecture. Every path is a potential landing page for a keyword cluster and should have its own title, meta description, H1, and schema markup.
| URL path                        | Primary keyword cluster                               | Content type           | Schema                        |
| ------------------------------- | ----------------------------------------------------- | ---------------------- | ----------------------------- |
| /what-is-an-ai-data-layer       | AI data layer, agentic data infrastructure            | Definition page        | FAQPage + SoftwareApplication |
| /mcp-enterprise-security        | MCP security enterprise, secure MCP server            | Technical guide        | HowTo + TechArticle           |
| /mcp-streamable-http-production | MCP streamable HTTP scaling, MCP long-running queries | Engineering blog       | TechArticle                   |
| /osi-execution-runtime          | OSI semantic layer runtime, OSI metric execution      | Standards commentary   | TechArticle                   |
| /firecracker-sandbox            | Firecracker AI sandbox, Python sandbox isolation      | Architecture deep-dive | TechArticle                   |
| /mcp-a2a-protocol-stack         | MCP A2A enterprise data, agentic AI protocol          | Explainer              | TechArticle                   |
| /vs                             | Strake vs \[competitors], alternative to NL2SQL       | Comparison hub         | FAQPage                       |
| /snowflake                      | AI agent Snowflake access, Snowflake AI runtime       | Runtime page           | HowTo + FAQPage               |
| /iceberg                        | Iceberg AI agent, Apache Iceberg AI query             | Runtime page           | HowTo + FAQPage               |
| /postgresql                     | PostgreSQL AI agent, AI agent database access         | Runtime page           | HowTo                         |
| /enterprise                     | Enterprise AI data governance, RBAC AI agents         | Commercial page        | SoftwareApplication           |
| /blog                           | All technical content hub                             | Blog index             | Blog + BreadcrumbList         |



#### 5.4 Core Web Vitals and Crawlability
LCP (Largest Contentful Paint) target: under 2.5 seconds. AI crawlers deprioritise slow pages in citation retrieval. Use Google Search Console's Core Web Vitals report to identify failing URLs.
Ensure sitemap.xml includes ALL blog posts and documentation pages — not just marketing pages. Submit to Google Search Console after every new post is published.
Canonical URLs: implement rel=canonical on every page. If docs and marketing share any content, canonicalise explicitly to avoid duplicate content penalties.
Internal linking: every technical blog post links to the relevant landing page AND to the 'What is an AI Data Layer?' definitional page. Hub-and-spoke internal link structure passes authority to the pages that matter most.
OpenGraph metadata on every page — developer Slack/Discord/HN sharing is Strake's primary distribution channel. The preview card is the first impression. Required: og:title, og:description, og:image (create a consistent template), og:type.
Measurement Framework
Two separate reporting streams are required — traditional SEO metrics and AI citation metrics. They measure different things and should not be collapsed into a single dashboard.

#### 6.1 Traditional SEO Dashboard (Monthly)
| Metric                               | Tool                           | Target (6 months)             | Notes                                                                                                 |
| ------------------------------------ | ------------------------------ | ----------------------------- | ----------------------------------------------------------------------------------------------------- |
| Keyword rankings — owned-now cluster | Ahrefs / Search Console        | Page 1 for 5+ terms           | Track: 'AI data layer', 'MCP enterprise security', 'federated SQL AI', 'Python sandbox data analysis' |
| Organic sessions to landing pages    | GA4                            | +400% from baseline           | Focus on /what-is-an-ai-data-layer, /mcp-enterprise-security, /vs                                     |
| Pages indexed                        | Search Console Coverage report | 100% of published pages       | Check weekly for indexing errors, especially after new posts                                          |
| Core Web Vitals — LCP                | Search Console CWV report      | All URLs: Good                | Must be <2.5s — critical for AI crawler priority                                                      |
| Tier-1 backlinks acquired            | Ahrefs new referring domains   | 3 confirmed by month 6        | Arrow, DataFusion, LanceDB are the tier-1 targets                                                     |
| Internal link coverage               | Screaming Frog crawl           | Every post links to hub pages | Monthly crawl to verify link structure is intact                                                      |


#### 6.2 AI Citation Dashboard (Monthly)
This is manual work at first — run target queries through each AI search engine and record whether Strake is cited, where in the answer, and with what framing. As of March 2026, no third-party tool reliably tracks AI citations across all engines. Build this tracking capability now before it becomes a paid feature.

| Query to test                               | Engine                      | Desired citation                         | Notes                                             |
| ------------------------------------------- | --------------------------- | ---------------------------------------- | ------------------------------------------------- |
| What is an AI data layer?                   | Perplexity, ChatGPT, Gemini | Strake cited as primary definition       | Target for month 2 after page 1.1 is indexed      |
| MCP enterprise security checklist           | Perplexity, ChatGPT         | Strake's checklist linked directly       | The 43% stat makes this highly citation-worthy    |
| How do AI agents access Snowflake securely? | Perplexity, Gemini          | Strake mentioned as runtime option       | Enterprise buyer query — high conversion intent   |
| MCP vs NL2SQL for enterprise data access    | ChatGPT, Gemini             | Strake cited in comparison               | The /vs page should trigger this                  |
| OSI semantic layer implementation tools     | Perplexity                  | Strake mentioned as execution runtime    | Phase 2 target — after OSI post is indexed        |
| Firecracker Python sandbox AI agents        | All engines                 | Strake cited as reference implementation | Highly specific — should be achievable in month 3 |
| AI agent data warehouse access              | All engines                 | Strake cited in the answer               | Broader query — longer timeline                   |


In Google Search Console, if a keyword's impressions rise while CTR falls, an AI Overview is appearing for that query. This is not a penalty — it means Strake's content is being consumed by the AI Overview. Monitor this signal monthly and use it to identify which queries are entering AI Mode territory. Adjust content to improve citation likelihood on those specific queries.
ChatGPT referral tracking
Since mid-2025, ChatGPT referral traffic appears in GA4 as utm_source=chatgpt. Set up a custom segment in GA4 to track this separately from organic search. Even a handful of referral visits from ChatGPT at the pre-revenue stage signals that AI systems are citing Strake in generated answers — this is a leading indicator that should be tracked from day one.

| KPI                                        | Target (3 months)          | Target (6 months)            | Target (12 months)                     |
| ------------------------------------------ | -------------------------- | ---------------------------- | -------------------------------------- |
| AI citations for 'AI data layer' query     | 1 engine mentions Strake   | Strake cited in 2+ engines   | Strake cited as primary definition     |
| AI citations for 'MCP enterprise security' | Perplexity cites checklist | All 3 engines cite checklist | Featured in AI Overview                |
| ChatGPT referral sessions / month          | Any detectable traffic     | 20+ sessions/month           | 100+ sessions/month                    |
| Tier-1 backlinks                           | 1 (OSI working group)      | 3 (+ DataFusion + LanceDB)   | 6 (+ Arrow + dbt + Pragmatic Engineer) |
| Target keyword page-1 rankings             | 2 terms page 1             | 5 terms page 1               | 10 terms page 1                        |

90-Day Execution Calendar
| Week       | Deliverable                                                                                                                         | Owner                 | Dependencies                                         | Success signal                                                                                 |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------- | --------------------- | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Week 1     | Audit robots.txt — allow all AI crawlers. Verify all pages indexed in Search Console. Install GA4 ChatGPT referral segment.         | Technical founder     | None                                                 | GPTBot, ClaudeBot allowed in robots.txt. 100% of current pages in sitemap.                     |
| Week 1–2   | Publish Page 1.1: 'What Is an AI Data Layer?' with FAQPage + Organization + SoftwareApplication schema.                             | Content + Technical   | 70-word opening paragraph drafted and approved       | Page indexed within 72 hours. FAQPage schema validates in Rich Results Test.                   |
| Week 2–3   | Implement Organization schema sitewide. Add Person schema to all existing blog posts with author bylines.                           | Technical             | Author GitHub/LinkedIn profiles verified             | Schema validates. Author entity appears in Google Search Console.                              |
| Week 3–4   | Publish Page 1.2: MCP Enterprise Security Checklist. Release GitHub Gist version in parallel.                                       | Content + Engineering | 12 checklist items technically reviewed and accurate | GitHub Gist gains 10+ stars within 2 weeks. Post shared in MCP Discord.                        |
| Week 4     | Submit OSI Working Group membership application.                                                                                    | Founder/BD            | None                                                 | Membership confirmed. Strake listed on OSI member page.                                        |
| Week 5–6   | Publish Page 1.3: MCP Streamable HTTP at Production Scale. Submit Show HN.                                                          | Engineering           | Code examples reviewed and tested                    | Show HN reaches front page or Ask HN response. 3+ comments from engineers confirming accuracy. |
| Week 6–7   | Publish Page 2.3: MCP + A2A Protocol Stack. Post to Agentic AI Foundation community.                                                | Content               | A2A protocol documentation reviewed                  | Post cited in A2A GitHub Discussions.                                                          |
| Week 7–8   | Internal linking audit: ensure every blog post and landing page links to /what-is-an-ai-data-layer. Verify sitemap is complete.     | Technical             | All Phase 1 pages published                          | Screaming Frog crawl shows every post has minimum 2 internal links to hub pages.               |
| Week 8–10  | Publish Page 2.2: Firecracker MicroVM sandbox deep-dive. Release open-source Seccomp profile to GitHub.                             | Engineering           | Benchmark data collected on representative workloads | GitHub repo earns 20+ stars. Post submitted to Firecracker GitHub Discussions.                 |
| Week 10–11 | Publish Page 2.1: OSI v1.0 and AI Agent Execution. Pitch to dbt Slack and data engineering communities.                             | Content               | OSI spec fully read and understood                   | Post referenced in at least one OSI-related community thread within 2 weeks.                   |
| Week 11–12 | Publish Page 2.4: Comparison Hub (/vs). Set up monthly AI citation tracking spreadsheet.                                            | Content + Analytics   | All comparison sections technically reviewed         | FAQPage schema validates. Manual AI citation testing begins.                                   |
| Week 12    | Month-1 review: run the 7 target queries through Perplexity, ChatGPT, Gemini. Record results. Identify which pages are being cited. | Analytics             | All Phase 1–2 pages indexed                          | Strake cited in at least 1 AI engine response for at least 1 target query.                     |


Content Templates & Writing Rules
These rules apply to every piece of content Strake publishes. They are derived from Google's May 2025 AI search guidance and the 2026 research on AI citation behaviour.

#### 7.1 The Opening Paragraph Rule
AI search engines extract the first complete sentence of each paragraph as a potential citation candidate. Every piece of Strake content must open with a sentence that:
Answers the primary query in full (who, what, how — all in one sentence)
Names the primary entity ('Strake' or the technology in question) explicitly in the first 10 words
Contains no hedging language ('may', 'might', 'could', 'potentially')
Is under 70 words — the observed extraction window for AI Overviews
Example — correct opening for the MCP security post:
"Deploying MCP in an enterprise environment requires security controls at three layers: network isolation, application-level prompt injection detection, and runtime sandboxing — because 43% of publicly available MCP servers currently contain command injection vulnerabilities."
This sentence: answers the question, names the domain (MCP enterprise security), contains a verifiable data point, and makes no hedging claims.

#### 7.2 Entity Consistency Rule
Every Strake content piece must refer to the product as 'Strake' or 'the Strake AI Data Layer' — never 'our platform', 'the solution', 'it', or 'our tool'. AI systems build entity graphs; inconsistent naming fragments the entity. The product name must appear in the first sentence of every page, in every H1, and in the meta description.

#### 7.3 Define Your Own Terms
Every proprietary term or architectural concept Strake uses (strake.search(), session taint tracking, federated session, zero-copy Arrow execution) must be defined on Strake's own website before anyone else defines it. Create a /glossary page or embed inline definitions in the relevant technical posts. AI systems cite the first clear definition they encountered — own these terms.

#### 7.4 The Two-Audience Rule
Every piece of Strake content serves two audiences simultaneously: the engineering buyer (hands-on, technical, searches Google for specific implementation problems) and the enterprise buyer (data platform lead / CISO, searches AI tools for vendor comparisons and governance answers). Structure content so the first 200 words work for the enterprise buyer and the technical sections work for the engineering buyer. Do not write two separate documents — write one document with clearly delineated sections.
Resource Requirements

| Activity                                                 | Time investment                                   | Who                        | When                 |
| -------------------------------------------------------- | ------------------------------------------------- | -------------------------- | -------------------- |
| robots.txt audit + AI crawler allowlist                  | 30 minutes                                        | Technical founder          | Day 1                |
| Organization + SoftwareApplication schema sitewide       | 2–3 hours                                         | Technical founder          | Week 1               |
| Page 1.1: AI Data Layer definition                       | 4–6 hours writing + 1 hour schema                 | Founder / technical writer | Week 1–2             |
| Page 1.2: MCP Security Checklist                         | 8–10 hours (writing + accuracy review)            | Engineering + content      | Week 3–4             |
| OSI working group membership                             | 2 hours to apply + 2 hours/month participation    | Founder                    | Week 4 + ongoing     |
| Page 1.3: MCP Streamable HTTP production                 | 6–8 hours (engineering-led post)                  | Engineering                | Week 5–6             |
| Page 2.2: Firecracker sandbox deep-dive + GitHub release | 10–12 hours (includes benchmark collection)       | Engineering                | Week 8–10            |
| Monthly AI citation tracking                             | 2 hours/month (7 queries × 3 engines + recording) | Content / analytics        | Monthly from week 12 |
| Monthly backlink outreach (1 target/month)               | 3–4 hours/month                                   | Founder / BD               | Monthly from Q2      |
| Page 2.1: OSI execution runtime                          | 6–8 hours writing                                 | Founder / content          | Week 10–11           |
| Page 2.4: Comparison hub                                 | 6–8 hours + technical accuracy review             | Content + engineering      | Week 11–12           |

Total Phase 1 investment estimate:
Approximately 45–60 hours of focused work across weeks 1–12, split roughly 60% engineering-led content and 40% strategic/schema/distribution. This is achievable for a founding team alongside product development — but only if content is treated as an engineering deliverable with defined completion criteria, not as a marketing task that gets deferred.
The Bottom Line: Priority Stack
Do these 5 things in the next 30 days:
Allow AI crawlers in robots.txt — 30 minutes, irreversible SEO harm if skipped
Publish 'What Is an AI Data Layer?' with full schema — the definition nobody else has written
Apply to join the OSI working group — earns a credibility backlink and strategic positioning
Implement Organization schema sitewide with sameAs links to GitHub and LinkedIn
Publish the MCP Enterprise Security Checklist anchored to the 43% vulnerability data point
Everything else in this document builds on these five actions. The category definition page and the MCP security checklist are the two pieces of content that can establish Strake as a cited authority in AI search within 60 days. No paid advertising, no PR budget, and no viral campaign is required — just technically honest content published in the right format at the right moment in a category that is defining itself in real time.
The window is open. It is measured in weeks for 'AI data layer' and 'MCP enterprise security'. It is measured in months for OSI, A2A+MCP, and the Firecracker isolation story. The only irreversible mistake is publishing nothing while competitors discover these gaps and fill them first.
